/* Copyright (c) Mark Harmstone 2024
 *
 * This file is part of tdscpp.
 *
 * tdscpp is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public Licence as published by
 * the Free Software Foundation, either version 3 of the Licence, or
 * (at your option) any later version.
 *
 * tdscpp is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public Licence for more details.
 *
 * You should have received a copy of the GNU Lesser General Public Licence
 * along with tdscpp.  If not, see <http://www.gnu.org/licenses/>. */

#ifdef _WIN32
#include <ws2tcpip.h>
#endif

#include "tdscpp.h"
#include "tdscpp-private.h"
#include "config.h"
#include "ringbuf.h"
#include <iostream>
#include <string>
#include <list>
#include <map>
#include <charconv>
#include <sys/types.h>
#include <nlohmann/json.hpp>

#ifndef _WIN32
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

#ifdef HAVE_GSSAPI
#include <gssapi/gssapi.h>
#endif

#include <unistd.h>
#else
#define SECURITY_WIN32
#include <windows.h>
#include <sspi.h>
#endif

// #define DEBUG_SHOW_MSGS

#ifndef _WIN32
#define CP_UTF8 65001
#include <unicode/ucnv.h>
#endif

using namespace std;

#define BROWSER_PORT 1434

static const uint32_t tds_74_version = 0x4000074;

static bool parse_row_col(enum tds::sql_type type, unsigned int max_length, span<const uint8_t>& sp) {
    switch (type) {
        case tds::sql_type::TINYINT:
        case tds::sql_type::BIT:
            if (sp.empty())
                return false;

            sp = sp.subspan(1);

            break;

        case tds::sql_type::SMALLINT:
            if (sp.size() < 2)
                return false;

            sp = sp.subspan(2);

            break;

        case tds::sql_type::INT:
        case tds::sql_type::DATETIM4:
        case tds::sql_type::SMALLMONEY:
        case tds::sql_type::REAL:
            if (sp.size() < 4)
                return false;

            sp = sp.subspan(4);

            break;

        case tds::sql_type::BIGINT:
        case tds::sql_type::DATETIME:
        case tds::sql_type::MONEY:
        case tds::sql_type::FLOAT:
            if (sp.size() < 8)
                return false;

            sp = sp.subspan(8);

            break;

        case tds::sql_type::SQL_NULL:
            break;

        case tds::sql_type::UNIQUEIDENTIFIER:
        case tds::sql_type::INTN:
        case tds::sql_type::DECIMAL:
        case tds::sql_type::NUMERIC:
        case tds::sql_type::BITN:
        case tds::sql_type::FLTN:
        case tds::sql_type::MONEYN:
        case tds::sql_type::DATETIMN:
        case tds::sql_type::DATE:
        case tds::sql_type::TIME:
        case tds::sql_type::DATETIME2:
        case tds::sql_type::DATETIMEOFFSET:
        {
            if (sp.size() < sizeof(uint8_t))
                return false;

            auto len = *(uint8_t*)sp.data();

            sp = sp.subspan(1);

            if (sp.size() < len)
                return false;

            sp = sp.subspan(len);

            break;
        }

        case tds::sql_type::VARCHAR:
        case tds::sql_type::NVARCHAR:
        case tds::sql_type::VARBINARY:
        case tds::sql_type::CHAR:
        case tds::sql_type::NCHAR:
        case tds::sql_type::BINARY:
        case tds::sql_type::XML:
        case tds::sql_type::UDT:
            if (max_length == 0xffff || type == tds::sql_type::XML || type == tds::sql_type::UDT) {
                if (sp.size() < sizeof(uint64_t))
                    return false;

                auto len = *(uint64_t*)sp.data();

                sp = sp.subspan(sizeof(uint64_t));

                if (len == 0xffffffffffffffff)
                    return true;

                do {
                    if (sp.size() < sizeof(uint32_t))
                        return false;

                    auto chunk_len = *(uint32_t*)sp.data();

                    sp = sp.subspan(sizeof(uint32_t));

                    if (chunk_len == 0)
                        break;

                    if (sp.size() < chunk_len)
                        return false;

                    sp = sp.subspan(chunk_len);
                } while (true);
            } else {
                if (sp.size() < sizeof(uint16_t))
                    return false;

                auto len = *(uint16_t*)sp.data();

                sp = sp.subspan(sizeof(uint16_t));

                if (len == 0xffff)
                    return true;

                if (sp.size() < len)
                    return false;

                sp = sp.subspan(len);
            }

            break;

        case tds::sql_type::SQL_VARIANT:
        {
            if (sp.size() < sizeof(uint32_t))
                return false;

            auto len = *(uint32_t*)sp.data();

            sp = sp.subspan(sizeof(uint32_t));

            if (len == 0xffffffff)
                return true;

            if (sp.size() < len)
                return false;

            sp = sp.subspan(len);

            break;
        }

        case tds::sql_type::IMAGE:
        case tds::sql_type::NTEXT:
        case tds::sql_type::TEXT:
        {
            // text pointer

            if (sp.size() < sizeof(uint8_t))
                return false;

            auto textptrlen = (uint8_t)sp[0];

            sp = sp.subspan(1);

            if (sp.size() < textptrlen)
                return false;

            sp = sp.subspan(textptrlen);

            if (textptrlen != 0) {
                // timestamp

                if (sp.size() < 8)
                    return false;

                sp = sp.subspan(8);

                // data

                if (sp.size() < sizeof(uint32_t))
                    return false;

                auto len = *(uint32_t*)sp.data();

                sp = sp.subspan(sizeof(uint32_t));

                if (sp.size() < len)
                    return false;

                sp = sp.subspan(len);
            }

            break;
        }

        default:
            throw formatted_error("Unhandled type {} in ROW message.", type);
    }

    return true;
}

span<const uint8_t> parse_tokens(span<const uint8_t> sp, list<vector<uint8_t>>& tokens, vector<tds::column>& buf_columns) {
    while (!sp.empty()) {
        auto type = (tds::token)sp[0];

        switch (type) {
            case tds::token::TABNAME:
            case tds::token::COLINFO:
            case tds::token::ORDER:
            case tds::token::TDS_ERROR:
            case tds::token::INFO:
            case tds::token::LOGINACK:
            case tds::token::ENVCHANGE:
            case tds::token::SSPI: {
                if (sp.size() < 1 + sizeof(uint16_t))
                    return sp;

                auto len = *(uint16_t*)&sp[1];

                if (sp.size() < (size_t)(1 + sizeof(uint16_t) + len))
                    return sp;

                tokens.emplace_back(sp.data(), sp.data() + 1 + sizeof(uint16_t) + len);
                sp = sp.subspan(1 + sizeof(uint16_t) + len);

                break;
            }

            case tds::token::DONE:
            case tds::token::DONEPROC:
            case tds::token::DONEINPROC:
                if (sp.size() < 1 + sizeof(tds_done_msg))
                    return sp;

                tokens.emplace_back(sp.data(), sp.data() + 1 + sizeof(tds_done_msg));
                sp = sp.subspan(1 + sizeof(tds_done_msg));
            break;

            case tds::token::COLMETADATA: {
                if (sp.size() < 5)
                    return sp;

                auto num_columns = *(uint16_t*)&sp[1];

                if (num_columns == 0) {
                    buf_columns.clear();
                    tokens.emplace_back(sp.data(), sp.data() + 5);
                    sp = sp.subspan(5);
                    continue;
                }

                vector<tds::column> cols;

                cols.reserve(num_columns);

                auto sp2 = sp.subspan(1 + sizeof(uint16_t));

                for (unsigned int i = 0; i < num_columns; i++) {
                    if (sp2.size() < sizeof(tds::tds_colmetadata_col))
                        return sp;

                    cols.emplace_back();

                    auto& col = cols.back();

                    auto& c = *(tds::tds_colmetadata_col*)&sp2[0];

                    col.type = c.type;

                    sp2 = sp2.subspan(sizeof(tds::tds_colmetadata_col));

                    switch (c.type) {
                        case tds::sql_type::SQL_NULL:
                        case tds::sql_type::TINYINT:
                        case tds::sql_type::BIT:
                        case tds::sql_type::SMALLINT:
                        case tds::sql_type::INT:
                        case tds::sql_type::DATETIM4:
                        case tds::sql_type::REAL:
                        case tds::sql_type::MONEY:
                        case tds::sql_type::DATETIME:
                        case tds::sql_type::FLOAT:
                        case tds::sql_type::SMALLMONEY:
                        case tds::sql_type::BIGINT:
                        case tds::sql_type::DATE:
                            // nop
                        break;

                        case tds::sql_type::INTN:
                        case tds::sql_type::FLTN:
                        case tds::sql_type::TIME:
                        case tds::sql_type::DATETIME2:
                        case tds::sql_type::DATETIMN:
                        case tds::sql_type::DATETIMEOFFSET:
                        case tds::sql_type::BITN:
                        case tds::sql_type::MONEYN:
                        case tds::sql_type::UNIQUEIDENTIFIER:
                            if (sp2.size() < sizeof(uint8_t))
                                return sp;

                            col.max_length = *(uint8_t*)sp2.data();

                            sp2 = sp2.subspan(1);
                        break;

                        case tds::sql_type::VARCHAR:
                        case tds::sql_type::NVARCHAR:
                        case tds::sql_type::CHAR:
                        case tds::sql_type::NCHAR:
                            if (sp2.size() < sizeof(uint16_t) + sizeof(tds::collation))
                                return sp;

                            col.max_length = *(uint16_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint16_t) + sizeof(tds::collation));
                        break;

                        case tds::sql_type::VARBINARY:
                        case tds::sql_type::BINARY:
                            if (sp2.size() < sizeof(uint16_t))
                                return sp;

                            col.max_length = *(uint16_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint16_t));
                        break;

                        case tds::sql_type::XML:
                            if (sp2.size() < sizeof(uint8_t))
                                return sp;

                            sp2 = sp2.subspan(sizeof(uint8_t));
                        break;

                        case tds::sql_type::DECIMAL:
                        case tds::sql_type::NUMERIC:
                            if (sp2.size() < 1)
                                return sp;

                            col.max_length = *(uint8_t*)sp2.data();

                            sp2 = sp2.subspan(1);

                            if (sp2.size() < 2)
                                return sp;

                            sp2 = sp2.subspan(2);
                        break;

                        case tds::sql_type::SQL_VARIANT:
                            if (sp2.size() < sizeof(uint32_t))
                                return sp;

                            col.max_length = *(uint32_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint32_t));
                        break;

                        case tds::sql_type::IMAGE:
                        case tds::sql_type::NTEXT:
                        case tds::sql_type::TEXT:
                        {
                            if (sp2.size() < sizeof(uint32_t))
                                return sp;

                            col.max_length = *(uint32_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint32_t));

                            if (c.type == tds::sql_type::TEXT || c.type == tds::sql_type::NTEXT) {
                                if (sp2.size() < sizeof(tds::collation))
                                    return sp;

                                sp2 = sp2.subspan(sizeof(tds::collation));
                            }

                            if (sp2.size() < 1)
                                return sp;

                            auto num_parts = (uint8_t)sp2[0];

                            sp2 = sp2.subspan(1);

                            for (uint8_t j = 0; j < num_parts; j++) {
                                if (sp2.size() < sizeof(uint16_t))
                                    return sp;

                                auto partlen = *(uint16_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint16_t));

                                if (sp2.size() < partlen * sizeof(char16_t))
                                    return sp;

                                sp2 = sp2.subspan(partlen * sizeof(char16_t));
                            }

                            break;
                        }

                        case tds::sql_type::UDT:
                        {
                            if (sp2.size() < sizeof(uint16_t))
                                return sp;

                            col.max_length = *(uint16_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint16_t));

                            if (sp2.size() < sizeof(uint8_t))
                                return sp;

                            // db name

                            auto string_len = *(uint8_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint8_t));

                            if (sp2.size() < string_len * sizeof(char16_t))
                                return sp;

                            sp2 = sp2.subspan(string_len * sizeof(char16_t));

                            // schema name

                            string_len = *(uint8_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint8_t));

                            if (sp2.size() < string_len * sizeof(char16_t))
                                return sp;

                            sp2 = sp2.subspan(string_len * sizeof(char16_t));

                            // type name

                            string_len = *(uint8_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint8_t));

                            if (sp2.size() < string_len * sizeof(char16_t))
                                return sp;

                            sp2 = sp2.subspan(string_len * sizeof(char16_t));

                            // assembly qualified name

                            auto string_len2 = *(uint16_t*)sp2.data();

                            sp2 = sp2.subspan(sizeof(uint16_t));

                            if (sp2.size() < string_len2 * sizeof(char16_t))
                                return sp;

                            sp2 = sp2.subspan(string_len2 * sizeof(char16_t));

                            break;
                        }

                        default:
                            throw formatted_error("Unhandled type {} in COLMETADATA message.", c.type);
                    }

                    if (sp2.size() < 1)
                        return sp;

                    auto name_len = (uint8_t)sp2[0];

                    sp2 = sp2.subspan(1);

                    if (sp2.size() < name_len * sizeof(char16_t))
                        return sp;

                    sp2 = sp2.subspan(name_len * sizeof(char16_t));
                }

                auto len = (size_t)(sp2.data() - sp.data());

                tokens.emplace_back(sp.data(), sp.data() + len);
                sp = sp.subspan(len);

                buf_columns = cols;

                break;
            }

            case tds::token::ROW: {
                auto sp2 = sp.subspan(1);

                for (unsigned int i = 0; i < buf_columns.size(); i++) {
                    if (!parse_row_col(buf_columns[i].type, buf_columns[i].max_length, sp2))
                        return sp;
                }

                auto len = (size_t)(sp2.data() - sp.data());

                tokens.emplace_back(sp.data(), sp.data() + len);
                sp = sp.subspan(len);

                break;
            }

            case tds::token::NBCROW:
            {
                if (buf_columns.empty())
                    break;

                auto sp2 = sp.subspan(1);

                auto bitset_length = (buf_columns.size() + 7) / 8;

                if (sp2.size() < bitset_length)
                    return sp;

                auto bitset = sp2.subspan(0, bitset_length);
                auto bsv = (uint8_t)bitset[0];

                sp2 = sp2.subspan(bitset_length);

                for (unsigned int i = 0; i < buf_columns.size(); i++) {
                    if (i != 0) {
                        if ((i & 7) == 0) {
                            bitset = bitset.subspan(1);
                            bsv = (uint8_t)bitset[0];
                        } else
                            bsv >>= 1;
                    }

                    if (!(bsv & 1)) { // not NULL
                        if (!parse_row_col(buf_columns[i].type, buf_columns[i].max_length, sp2))
                            return sp;
                    }
                }

                auto len = (size_t)(sp2.data() - sp.data());

                tokens.emplace_back(sp.data(), sp.data() + len);
                sp = sp.subspan(len);

                break;
            }

            case tds::token::RETURNSTATUS:
            {
                if (sp.size() < 1 + sizeof(int32_t))
                    return sp;

                tokens.emplace_back(sp.data(), sp.data() + 1 + sizeof(int32_t));
                sp = sp.subspan(1 + sizeof(int32_t));

                break;
            }

            case tds::token::RETURNVALUE:
            {
                auto h = (tds_return_value*)&sp[1];

                if (sp.size() < 1 + sizeof(tds_return_value))
                    return sp;

                // FIXME - param name

                if (is_byte_len_type(h->type)) {
                    uint8_t len;

                    if (sp.size() < 1 + sizeof(tds_return_value) + 2)
                        return sp;

                    len = *(&sp[1] + sizeof(tds_return_value) + 1);

                    if (sp.size() < 1 + sizeof(tds_return_value) + 2 + len)
                        return sp;

                    tokens.emplace_back(sp.data(), sp.data() + 1 + sizeof(tds_return_value) + 2 + len);
                    sp = sp.subspan(1 + sizeof(tds_return_value) + 2 + len);
                } else
                    throw formatted_error("Unhandled type {} in RETURNVALUE message.", h->type);

                break;
            }

            case tds::token::FEATUREEXTACK:
            {
                auto sp2 = sp.subspan(1);

                while (true) {
                    if (sp2.size() < 1)
                        return sp;

                    if ((uint8_t)sp2[0] == 0xff) {
                        sp2 = sp2.subspan(1);
                        break;
                    }

                    if (sp2.size() < 1 + sizeof(uint32_t))
                        return sp;

                    auto len = *(uint32_t*)&sp2[1];

                    sp2 = sp2.subspan(1 + sizeof(uint32_t));

                    if (sp2.size() < len)
                        return sp;

                    sp2 = sp2.subspan(len);
                }

                auto token_len = (size_t)(sp2.data() - sp.data());

                tokens.emplace_back(sp.data(), sp.data() + token_len);
                sp = sp.subspan(token_len);

                break;
            }

            default:
                throw formatted_error("Unhandled token type {} while parsing tokens.", type);
        }
    }

    return sp;
}

void handle_row_col(tds::value_data_t& val, bool& is_null, enum tds::sql_type type,
                    unsigned int max_length, span<const uint8_t>& sp) {
    switch (type) {
        case tds::sql_type::TINYINT:
        case tds::sql_type::BIT:
            if (sp.empty())
                throw formatted_error("Short ROW message ({} bytes left, expected at least 1).", sp.size());

            val.assign(sp.data(), sp.data() + 1);

            sp = sp.subspan(1);

            break;

        case tds::sql_type::SMALLINT:
            if (sp.size() < 2)
                throw formatted_error("Short ROW message ({} bytes left, expected at least 2).", sp.size());

            val.assign(sp.data(), sp.data() + 2);

            sp = sp.subspan(2);

            break;

        case tds::sql_type::INT:
        case tds::sql_type::DATETIM4:
        case tds::sql_type::SMALLMONEY:
        case tds::sql_type::REAL:
            if (sp.size() < 4)
                throw formatted_error("Short ROW message ({} bytes left, expected at least 4).", sp.size());

            val.assign(sp.data(), sp.data() + 4);

            sp = sp.subspan(4);

            break;

        case tds::sql_type::BIGINT:
        case tds::sql_type::DATETIME:
        case tds::sql_type::MONEY:
        case tds::sql_type::FLOAT:
            if (sp.size() < 8)
                throw formatted_error("Short ROW message ({} bytes left, expected at least 8).", sp.size());

            val.assign(sp.data(), sp.data() + 8);

            sp = sp.subspan(8);

            break;

        case tds::sql_type::SQL_NULL:
            val.clear();
            break;

        case tds::sql_type::UNIQUEIDENTIFIER:
        case tds::sql_type::INTN:
        case tds::sql_type::DECIMAL:
        case tds::sql_type::NUMERIC:
        case tds::sql_type::BITN:
        case tds::sql_type::FLTN:
        case tds::sql_type::MONEYN:
        case tds::sql_type::DATETIMN:
        case tds::sql_type::DATE:
        case tds::sql_type::TIME:
        case tds::sql_type::DATETIME2:
        case tds::sql_type::DATETIMEOFFSET:
        {
            if (sp.size() < sizeof(uint8_t))
                throw formatted_error("Short ROW message ({} bytes left, expected at least 1).", sp.size());

            auto len = *(uint8_t*)sp.data();

            sp = sp.subspan(1);

            if (sp.size() < len)
                throw formatted_error("Short ROW message ({} bytes left, expected at least {}).", sp.size(), len);

            val.resize(len);
            is_null = len == 0;

            memcpy(val.data(), sp.data(), len);
            sp = sp.subspan(len);

            break;
        }

        case tds::sql_type::VARCHAR:
        case tds::sql_type::NVARCHAR:
        case tds::sql_type::VARBINARY:
        case tds::sql_type::CHAR:
        case tds::sql_type::NCHAR:
        case tds::sql_type::BINARY:
        case tds::sql_type::XML:
        case tds::sql_type::UDT:
            if (max_length == 0xffff || type == tds::sql_type::XML || type == tds::sql_type::UDT) {
                if (sp.size() < sizeof(uint64_t))
                    throw formatted_error("Short ROW message ({} bytes left, expected at least 8).", sp.size());

                auto len = *(uint64_t*)sp.data();

                sp = sp.subspan(sizeof(uint64_t));

                val.clear();

                if (len == 0xffffffffffffffff) {
                    is_null = true;
                    return;
                }

                is_null = false;

                // FIXME - set vector length first of all

                if (len != 0xfffffffffffffffe) // unknown length
                    val.reserve((size_t)len);

                do {
                    if (sp.size() < sizeof(uint32_t))
                        throw formatted_error("Short ROW message ({} bytes left, expected at least 4).", sp.size());

                    auto chunk_len = *(uint32_t*)sp.data();

                    sp = sp.subspan(sizeof(uint32_t));

                    if (chunk_len == 0)
                        break;

                    if (sp.size() < chunk_len)
                        throw formatted_error("Short ROW message ({} bytes left, expected at least {}).", sp.size(), chunk_len);

                    val.resize(val.size() + chunk_len);
                    memcpy(val.data() + val.size() - chunk_len, sp.data(), chunk_len);
                    sp = sp.subspan(chunk_len);
                } while (true);
            } else {
                if (sp.size() < sizeof(uint16_t))
                    throw formatted_error("Short ROW message ({} bytes left, expected at least 2).", sp.size());

                auto len = *(uint16_t*)sp.data();

                sp = sp.subspan(sizeof(uint16_t));

                if (len == 0xffff) {
                    is_null = true;
                    return;
                }

                val.resize(len);
                is_null = false;

                if (sp.size() < len)
                    throw formatted_error("Short ROW message ({} bytes left, expected at least {}).", sp.size(), len);

                memcpy(val.data(), sp.data(), len);
                sp = sp.subspan(len);
            }

            break;

        case tds::sql_type::SQL_VARIANT:
        {
            if (sp.size() < sizeof(uint32_t))
                throw formatted_error("Short ROW message ({} bytes left, expected at least 4).", sp.size());

            auto len = *(uint32_t*)sp.data();

            sp = sp.subspan(sizeof(uint32_t));

            val.resize(len);
            is_null = len == 0xffffffff;

            if (!is_null) {
                if (sp.size() < len)
                    throw formatted_error("Short ROW message ({} bytes left, expected at least {}).", sp.size(), len);

                memcpy(val.data(), sp.data(), len);
                sp = sp.subspan(len);
            }

            break;
        }

        case tds::sql_type::IMAGE:
        case tds::sql_type::NTEXT:
        case tds::sql_type::TEXT:
        {
            // text pointer

            if (sp.size() < sizeof(uint8_t))
                throw formatted_error("Short ROW message ({} bytes left, expected at least 1).", sp.size());

            auto textptrlen = (uint8_t)sp[0];

            sp = sp.subspan(1);

            if (sp.size() < textptrlen)
                throw formatted_error("Short ROW message ({} bytes left, expected at least {}).", sp.size(), textptrlen);

            sp = sp.subspan(textptrlen);

            is_null = textptrlen == 0;

            if (!is_null) {
                // timestamp

                if (sp.size() < 8)
                    throw formatted_error("Short ROW message ({} bytes left, expected at least 8).", sp.size());

                sp = sp.subspan(8);

                // data

                if (sp.size() < sizeof(uint32_t))
                    throw formatted_error("Short ROW message ({} bytes left, expected at least 4).", sp.size());

                auto len = *(uint32_t*)sp.data();

                sp = sp.subspan(sizeof(uint32_t));

                val.resize(len);
                is_null = len == 0xffffffff;

                if (!is_null) {
                    if (sp.size() < len)
                        throw formatted_error("Short ROW message ({} bytes left, expected at least {}).", sp.size(), len);

                    memcpy(val.data(), sp.data(), len);
                    sp = sp.subspan(len);
                }
            }

            break;
        }

        default:
            throw formatted_error("Unhandled type {} in ROW message.", type);
    }
}

#ifdef HAVE_GSSAPI
template<>
struct std::formatter<enum krb5_minor> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum krb5_minor t, format_context& ctx) const {
        switch (t) {
            case krb5_minor::KRB5KDC_ERR_NONE:
                return format_to(ctx.out(), "KRB5KDC_ERR_NONE");

            case krb5_minor::KRB5KDC_ERR_NAME_EXP:
                return format_to(ctx.out(), "KRB5KDC_ERR_NAME_EXP");

            case krb5_minor::KRB5KDC_ERR_SERVICE_EXP:
                return format_to(ctx.out(), "KRB5KDC_ERR_SERVICE_EXP");

            case krb5_minor::KRB5KDC_ERR_BAD_PVNO:
                return format_to(ctx.out(), "KRB5KDC_ERR_BAD_PVNO");

            case krb5_minor::KRB5KDC_ERR_C_OLD_MAST_KVNO:
                return format_to(ctx.out(), "KRB5KDC_ERR_C_OLD_MAST_KVNO");

            case krb5_minor::KRB5KDC_ERR_S_OLD_MAST_KVNO:
                return format_to(ctx.out(), "KRB5KDC_ERR_S_OLD_MAST_KVNO");

            case krb5_minor::KRB5KDC_ERR_C_PRINCIPAL_UNKNOWN:
                return format_to(ctx.out(), "KRB5KDC_ERR_C_PRINCIPAL_UNKNOWN");

            case krb5_minor::KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN:
                return format_to(ctx.out(), "KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN");

            case krb5_minor::KRB5KDC_ERR_PRINCIPAL_NOT_UNIQUE:
                return format_to(ctx.out(), "KRB5KDC_ERR_PRINCIPAL_NOT_UNIQUE");

            case krb5_minor::KRB5KDC_ERR_NULL_KEY:
                return format_to(ctx.out(), "KRB5KDC_ERR_NULL_KEY");

            case krb5_minor::KRB5KDC_ERR_CANNOT_POSTDATE:
                return format_to(ctx.out(), "KRB5KDC_ERR_CANNOT_POSTDATE");

            case krb5_minor::KRB5KDC_ERR_NEVER_VALID:
                return format_to(ctx.out(), "KRB5KDC_ERR_NEVER_VALID");

            case krb5_minor::KRB5KDC_ERR_POLICY:
                return format_to(ctx.out(), "KRB5KDC_ERR_POLICY");

            case krb5_minor::KRB5KDC_ERR_BADOPTION:
                return format_to(ctx.out(), "KRB5KDC_ERR_BADOPTION");

            case krb5_minor::KRB5KDC_ERR_ETYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5KDC_ERR_ETYPE_NOSUPP");

            case krb5_minor::KRB5KDC_ERR_SUMTYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5KDC_ERR_SUMTYPE_NOSUPP");

            case krb5_minor::KRB5KDC_ERR_PADATA_TYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5KDC_ERR_PADATA_TYPE_NOSUPP");

            case krb5_minor::KRB5KDC_ERR_TRTYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5KDC_ERR_TRTYPE_NOSUPP");

            case krb5_minor::KRB5KDC_ERR_CLIENT_REVOKED:
                return format_to(ctx.out(), "KRB5KDC_ERR_CLIENT_REVOKED");

            case krb5_minor::KRB5KDC_ERR_SERVICE_REVOKED:
                return format_to(ctx.out(), "KRB5KDC_ERR_SERVICE_REVOKED");

            case krb5_minor::KRB5KDC_ERR_TGT_REVOKED:
                return format_to(ctx.out(), "KRB5KDC_ERR_TGT_REVOKED");

            case krb5_minor::KRB5KDC_ERR_CLIENT_NOTYET:
                return format_to(ctx.out(), "KRB5KDC_ERR_CLIENT_NOTYET");

            case krb5_minor::KRB5KDC_ERR_SERVICE_NOTYET:
                return format_to(ctx.out(), "KRB5KDC_ERR_SERVICE_NOTYET");

            case krb5_minor::KRB5KDC_ERR_KEY_EXP:
                return format_to(ctx.out(), "KRB5KDC_ERR_KEY_EXP");

            case krb5_minor::KRB5KDC_ERR_PREAUTH_FAILED:
                return format_to(ctx.out(), "KRB5KDC_ERR_PREAUTH_FAILED");

            case krb5_minor::KRB5KDC_ERR_PREAUTH_REQUIRED:
                return format_to(ctx.out(), "KRB5KDC_ERR_PREAUTH_REQUIRED");

            case krb5_minor::KRB5KDC_ERR_SERVER_NOMATCH:
                return format_to(ctx.out(), "KRB5KDC_ERR_SERVER_NOMATCH");

            case krb5_minor::KRB5KDC_ERR_MUST_USE_USER2USER:
                return format_to(ctx.out(), "KRB5KDC_ERR_MUST_USE_USER2USER");

            case krb5_minor::KRB5KDC_ERR_PATH_NOT_ACCEPTED:
                return format_to(ctx.out(), "KRB5KDC_ERR_PATH_NOT_ACCEPTED");

            case krb5_minor::KRB5KDC_ERR_SVC_UNAVAILABLE:
                return format_to(ctx.out(), "KRB5KDC_ERR_SVC_UNAVAILABLE");

            case krb5_minor::KRB5PLACEHOLD_30:
                return format_to(ctx.out(), "KRB5PLACEHOLD_30");

            case krb5_minor::KRB5KRB_AP_ERR_BAD_INTEGRITY:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BAD_INTEGRITY");

            case krb5_minor::KRB5KRB_AP_ERR_TKT_EXPIRED:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_TKT_EXPIRED");

            case krb5_minor::KRB5KRB_AP_ERR_TKT_NYV:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_TKT_NYV");

            case krb5_minor::KRB5KRB_AP_ERR_REPEAT:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_REPEAT");

            case krb5_minor::KRB5KRB_AP_ERR_NOT_US:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_NOT_US");

            case krb5_minor::KRB5KRB_AP_ERR_BADMATCH:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BADMATCH");

            case krb5_minor::KRB5KRB_AP_ERR_SKEW:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_SKEW");

            case krb5_minor::KRB5KRB_AP_ERR_BADADDR:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BADADDR");

            case krb5_minor::KRB5KRB_AP_ERR_BADVERSION:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BADVERSION");

            case krb5_minor::KRB5KRB_AP_ERR_MSG_TYPE:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_MSG_TYPE");

            case krb5_minor::KRB5KRB_AP_ERR_MODIFIED:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_MODIFIED");

            case krb5_minor::KRB5KRB_AP_ERR_BADORDER:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BADORDER");

            case krb5_minor::KRB5KRB_AP_ERR_ILL_CR_TKT:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_ILL_CR_TKT");

            case krb5_minor::KRB5KRB_AP_ERR_BADKEYVER:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BADKEYVER");

            case krb5_minor::KRB5KRB_AP_ERR_NOKEY:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_NOKEY");

            case krb5_minor::KRB5KRB_AP_ERR_MUT_FAIL:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_MUT_FAIL");

            case krb5_minor::KRB5KRB_AP_ERR_BADDIRECTION:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BADDIRECTION");

            case krb5_minor::KRB5KRB_AP_ERR_METHOD:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_METHOD");

            case krb5_minor::KRB5KRB_AP_ERR_BADSEQ:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_BADSEQ");

            case krb5_minor::KRB5KRB_AP_ERR_INAPP_CKSUM:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_INAPP_CKSUM");

            case krb5_minor::KRB5KRB_AP_PATH_NOT_ACCEPTED:
                return format_to(ctx.out(), "KRB5KRB_AP_PATH_NOT_ACCEPTED");

            case krb5_minor::KRB5KRB_ERR_RESPONSE_TOO_BIG:
                return format_to(ctx.out(), "KRB5KRB_ERR_RESPONSE_TOO_BIG");

            case krb5_minor::KRB5PLACEHOLD_53:
                return format_to(ctx.out(), "KRB5PLACEHOLD_53");

            case krb5_minor::KRB5PLACEHOLD_54:
                return format_to(ctx.out(), "KRB5PLACEHOLD_54");

            case krb5_minor::KRB5PLACEHOLD_55:
                return format_to(ctx.out(), "KRB5PLACEHOLD_55");

            case krb5_minor::KRB5PLACEHOLD_56:
                return format_to(ctx.out(), "KRB5PLACEHOLD_56");

            case krb5_minor::KRB5PLACEHOLD_57:
                return format_to(ctx.out(), "KRB5PLACEHOLD_57");

            case krb5_minor::KRB5PLACEHOLD_58:
                return format_to(ctx.out(), "KRB5PLACEHOLD_58");

            case krb5_minor::KRB5PLACEHOLD_59:
                return format_to(ctx.out(), "KRB5PLACEHOLD_59");

            case krb5_minor::KRB5KRB_ERR_GENERIC:
                return format_to(ctx.out(), "KRB5KRB_ERR_GENERIC");

            case krb5_minor::KRB5KRB_ERR_FIELD_TOOLONG:
                return format_to(ctx.out(), "KRB5KRB_ERR_FIELD_TOOLONG");

            case krb5_minor::KRB5KDC_ERR_CLIENT_NOT_TRUSTED:
                return format_to(ctx.out(), "KRB5KDC_ERR_CLIENT_NOT_TRUSTED");

            case krb5_minor::KRB5KDC_ERR_KDC_NOT_TRUSTED:
                return format_to(ctx.out(), "KRB5KDC_ERR_KDC_NOT_TRUSTED");

            case krb5_minor::KRB5KDC_ERR_INVALID_SIG:
                return format_to(ctx.out(), "KRB5KDC_ERR_INVALID_SIG");

            case krb5_minor::KRB5KDC_ERR_DH_KEY_PARAMETERS_NOT_ACCEPTED:
                return format_to(ctx.out(), "KRB5KDC_ERR_DH_KEY_PARAMETERS_NOT_ACCEPTED");

            case krb5_minor::KRB5KDC_ERR_CERTIFICATE_MISMATCH:
                return format_to(ctx.out(), "KRB5KDC_ERR_CERTIFICATE_MISMATCH");

            case krb5_minor::KRB5KRB_AP_ERR_NO_TGT:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_NO_TGT");

            case krb5_minor::KRB5KDC_ERR_WRONG_REALM:
                return format_to(ctx.out(), "KRB5KDC_ERR_WRONG_REALM");

            case krb5_minor::KRB5KRB_AP_ERR_USER_TO_USER_REQUIRED:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_USER_TO_USER_REQUIRED");

            case krb5_minor::KRB5KDC_ERR_CANT_VERIFY_CERTIFICATE:
                return format_to(ctx.out(), "KRB5KDC_ERR_CANT_VERIFY_CERTIFICATE");

            case krb5_minor::KRB5KDC_ERR_INVALID_CERTIFICATE:
                return format_to(ctx.out(), "KRB5KDC_ERR_INVALID_CERTIFICATE");

            case krb5_minor::KRB5KDC_ERR_REVOKED_CERTIFICATE:
                return format_to(ctx.out(), "KRB5KDC_ERR_REVOKED_CERTIFICATE");

            case krb5_minor::KRB5KDC_ERR_REVOCATION_STATUS_UNKNOWN:
                return format_to(ctx.out(), "KRB5KDC_ERR_REVOCATION_STATUS_UNKNOWN");

            case krb5_minor::KRB5KDC_ERR_REVOCATION_STATUS_UNAVAILABLE:
                return format_to(ctx.out(), "KRB5KDC_ERR_REVOCATION_STATUS_UNAVAILABLE");

            case krb5_minor::KRB5KDC_ERR_CLIENT_NAME_MISMATCH:
                return format_to(ctx.out(), "KRB5KDC_ERR_CLIENT_NAME_MISMATCH");

            case krb5_minor::KRB5KDC_ERR_KDC_NAME_MISMATCH:
                return format_to(ctx.out(), "KRB5KDC_ERR_KDC_NAME_MISMATCH");

            case krb5_minor::KRB5KDC_ERR_INCONSISTENT_KEY_PURPOSE:
                return format_to(ctx.out(), "KRB5KDC_ERR_INCONSISTENT_KEY_PURPOSE");

            case krb5_minor::KRB5KDC_ERR_DIGEST_IN_CERT_NOT_ACCEPTED:
                return format_to(ctx.out(), "KRB5KDC_ERR_DIGEST_IN_CERT_NOT_ACCEPTED");

            case krb5_minor::KRB5KDC_ERR_PA_CHECKSUM_MUST_BE_INCLUDED:
                return format_to(ctx.out(), "KRB5KDC_ERR_PA_CHECKSUM_MUST_BE_INCLUDED");

            case krb5_minor::KRB5KDC_ERR_DIGEST_IN_SIGNED_DATA_NOT_ACCEPTED:
                return format_to(ctx.out(), "KRB5KDC_ERR_DIGEST_IN_SIGNED_DATA_NOT_ACCEPTED");

            case krb5_minor::KRB5KDC_ERR_PUBLIC_KEY_ENCRYPTION_NOT_SUPPORTED:
                return format_to(ctx.out(), "KRB5KDC_ERR_PUBLIC_KEY_ENCRYPTION_NOT_SUPPORTED");

            case krb5_minor::KRB5PLACEHOLD_82:
                return format_to(ctx.out(), "KRB5PLACEHOLD_82");

            case krb5_minor::KRB5PLACEHOLD_83:
                return format_to(ctx.out(), "KRB5PLACEHOLD_83");

            case krb5_minor::KRB5PLACEHOLD_84:
                return format_to(ctx.out(), "KRB5PLACEHOLD_84");

            case krb5_minor::KRB5KRB_AP_ERR_IAKERB_KDC_NOT_FOUND:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_IAKERB_KDC_NOT_FOUND");

            case krb5_minor::KRB5KRB_AP_ERR_IAKERB_KDC_NO_RESPONSE:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_IAKERB_KDC_NO_RESPONSE");

            case krb5_minor::KRB5PLACEHOLD_87:
                return format_to(ctx.out(), "KRB5PLACEHOLD_87");

            case krb5_minor::KRB5PLACEHOLD_88:
                return format_to(ctx.out(), "KRB5PLACEHOLD_88");

            case krb5_minor::KRB5PLACEHOLD_89:
                return format_to(ctx.out(), "KRB5PLACEHOLD_89");

            case krb5_minor::KRB5KDC_ERR_PREAUTH_EXPIRED:
                return format_to(ctx.out(), "KRB5KDC_ERR_PREAUTH_EXPIRED");

            case krb5_minor::KRB5KDC_ERR_MORE_PREAUTH_DATA_REQUIRED:
                return format_to(ctx.out(), "KRB5KDC_ERR_MORE_PREAUTH_DATA_REQUIRED");

            case krb5_minor::KRB5PLACEHOLD_92:
                return format_to(ctx.out(), "KRB5PLACEHOLD_92");

            case krb5_minor::KRB5KDC_ERR_UNKNOWN_CRITICAL_FAST_OPTION:
                return format_to(ctx.out(), "KRB5KDC_ERR_UNKNOWN_CRITICAL_FAST_OPTION");

            case krb5_minor::KRB5PLACEHOLD_94:
                return format_to(ctx.out(), "KRB5PLACEHOLD_94");

            case krb5_minor::KRB5PLACEHOLD_95:
                return format_to(ctx.out(), "KRB5PLACEHOLD_95");

            case krb5_minor::KRB5PLACEHOLD_96:
                return format_to(ctx.out(), "KRB5PLACEHOLD_96");

            case krb5_minor::KRB5PLACEHOLD_97:
                return format_to(ctx.out(), "KRB5PLACEHOLD_97");

            case krb5_minor::KRB5PLACEHOLD_98:
                return format_to(ctx.out(), "KRB5PLACEHOLD_98");

            case krb5_minor::KRB5PLACEHOLD_99:
                return format_to(ctx.out(), "KRB5PLACEHOLD_99");

            case krb5_minor::KRB5KDC_ERR_NO_ACCEPTABLE_KDF:
                return format_to(ctx.out(), "KRB5KDC_ERR_NO_ACCEPTABLE_KDF");

            case krb5_minor::KRB5PLACEHOLD_101:
                return format_to(ctx.out(), "KRB5PLACEHOLD_101");

            case krb5_minor::KRB5PLACEHOLD_102:
                return format_to(ctx.out(), "KRB5PLACEHOLD_102");

            case krb5_minor::KRB5PLACEHOLD_103:
                return format_to(ctx.out(), "KRB5PLACEHOLD_103");

            case krb5_minor::KRB5PLACEHOLD_104:
                return format_to(ctx.out(), "KRB5PLACEHOLD_104");

            case krb5_minor::KRB5PLACEHOLD_105:
                return format_to(ctx.out(), "KRB5PLACEHOLD_105");

            case krb5_minor::KRB5PLACEHOLD_106:
                return format_to(ctx.out(), "KRB5PLACEHOLD_106");

            case krb5_minor::KRB5PLACEHOLD_107:
                return format_to(ctx.out(), "KRB5PLACEHOLD_107");

            case krb5_minor::KRB5PLACEHOLD_108:
                return format_to(ctx.out(), "KRB5PLACEHOLD_108");

            case krb5_minor::KRB5PLACEHOLD_109:
                return format_to(ctx.out(), "KRB5PLACEHOLD_109");

            case krb5_minor::KRB5PLACEHOLD_110:
                return format_to(ctx.out(), "KRB5PLACEHOLD_110");

            case krb5_minor::KRB5PLACEHOLD_111:
                return format_to(ctx.out(), "KRB5PLACEHOLD_111");

            case krb5_minor::KRB5PLACEHOLD_112:
                return format_to(ctx.out(), "KRB5PLACEHOLD_112");

            case krb5_minor::KRB5PLACEHOLD_113:
                return format_to(ctx.out(), "KRB5PLACEHOLD_113");

            case krb5_minor::KRB5PLACEHOLD_114:
                return format_to(ctx.out(), "KRB5PLACEHOLD_114");

            case krb5_minor::KRB5PLACEHOLD_115:
                return format_to(ctx.out(), "KRB5PLACEHOLD_115");

            case krb5_minor::KRB5PLACEHOLD_116:
                return format_to(ctx.out(), "KRB5PLACEHOLD_116");

            case krb5_minor::KRB5PLACEHOLD_117:
                return format_to(ctx.out(), "KRB5PLACEHOLD_117");

            case krb5_minor::KRB5PLACEHOLD_118:
                return format_to(ctx.out(), "KRB5PLACEHOLD_118");

            case krb5_minor::KRB5PLACEHOLD_119:
                return format_to(ctx.out(), "KRB5PLACEHOLD_119");

            case krb5_minor::KRB5PLACEHOLD_120:
                return format_to(ctx.out(), "KRB5PLACEHOLD_120");

            case krb5_minor::KRB5PLACEHOLD_121:
                return format_to(ctx.out(), "KRB5PLACEHOLD_121");

            case krb5_minor::KRB5PLACEHOLD_122:
                return format_to(ctx.out(), "KRB5PLACEHOLD_122");

            case krb5_minor::KRB5PLACEHOLD_123:
                return format_to(ctx.out(), "KRB5PLACEHOLD_123");

            case krb5_minor::KRB5PLACEHOLD_124:
                return format_to(ctx.out(), "KRB5PLACEHOLD_124");

            case krb5_minor::KRB5PLACEHOLD_125:
                return format_to(ctx.out(), "KRB5PLACEHOLD_125");

            case krb5_minor::KRB5PLACEHOLD_126:
                return format_to(ctx.out(), "KRB5PLACEHOLD_126");

            case krb5_minor::KRB5PLACEHOLD_127:
                return format_to(ctx.out(), "KRB5PLACEHOLD_127");

            case krb5_minor::KRB5_ERR_RCSID:
                return format_to(ctx.out(), "KRB5_ERR_RCSID");

            case krb5_minor::KRB5_LIBOS_BADLOCKFLAG:
                return format_to(ctx.out(), "KRB5_LIBOS_BADLOCKFLAG");

            case krb5_minor::KRB5_LIBOS_CANTREADPWD:
                return format_to(ctx.out(), "KRB5_LIBOS_CANTREADPWD");

            case krb5_minor::KRB5_LIBOS_BADPWDMATCH:
                return format_to(ctx.out(), "KRB5_LIBOS_BADPWDMATCH");

            case krb5_minor::KRB5_LIBOS_PWDINTR:
                return format_to(ctx.out(), "KRB5_LIBOS_PWDINTR");

            case krb5_minor::KRB5_PARSE_ILLCHAR:
                return format_to(ctx.out(), "KRB5_PARSE_ILLCHAR");

            case krb5_minor::KRB5_PARSE_MALFORMED:
                return format_to(ctx.out(), "KRB5_PARSE_MALFORMED");

            case krb5_minor::KRB5_CONFIG_CANTOPEN:
                return format_to(ctx.out(), "KRB5_CONFIG_CANTOPEN");

            case krb5_minor::KRB5_CONFIG_BADFORMAT:
                return format_to(ctx.out(), "KRB5_CONFIG_BADFORMAT");

            case krb5_minor::KRB5_CONFIG_NOTENUFSPACE:
                return format_to(ctx.out(), "KRB5_CONFIG_NOTENUFSPACE");

            case krb5_minor::KRB5_BADMSGTYPE:
                return format_to(ctx.out(), "KRB5_BADMSGTYPE");

            case krb5_minor::KRB5_CC_BADNAME:
                return format_to(ctx.out(), "KRB5_CC_BADNAME");

            case krb5_minor::KRB5_CC_UNKNOWN_TYPE:
                return format_to(ctx.out(), "KRB5_CC_UNKNOWN_TYPE");

            case krb5_minor::KRB5_CC_NOTFOUND:
                return format_to(ctx.out(), "KRB5_CC_NOTFOUND");

            case krb5_minor::KRB5_CC_END:
                return format_to(ctx.out(), "KRB5_CC_END");

            case krb5_minor::KRB5_NO_TKT_SUPPLIED:
                return format_to(ctx.out(), "KRB5_NO_TKT_SUPPLIED");

            case krb5_minor::KRB5KRB_AP_WRONG_PRINC:
                return format_to(ctx.out(), "KRB5KRB_AP_WRONG_PRINC");

            case krb5_minor::KRB5KRB_AP_ERR_TKT_INVALID:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_TKT_INVALID");

            case krb5_minor::KRB5_PRINC_NOMATCH:
                return format_to(ctx.out(), "KRB5_PRINC_NOMATCH");

            case krb5_minor::KRB5_KDCREP_MODIFIED:
                return format_to(ctx.out(), "KRB5_KDCREP_MODIFIED");

            case krb5_minor::KRB5_KDCREP_SKEW:
                return format_to(ctx.out(), "KRB5_KDCREP_SKEW");

            case krb5_minor::KRB5_IN_TKT_REALM_MISMATCH:
                return format_to(ctx.out(), "KRB5_IN_TKT_REALM_MISMATCH");

            case krb5_minor::KRB5_PROG_ETYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5_PROG_ETYPE_NOSUPP");

            case krb5_minor::KRB5_PROG_KEYTYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5_PROG_KEYTYPE_NOSUPP");

            case krb5_minor::KRB5_WRONG_ETYPE:
                return format_to(ctx.out(), "KRB5_WRONG_ETYPE");

            case krb5_minor::KRB5_PROG_SUMTYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5_PROG_SUMTYPE_NOSUPP");

            case krb5_minor::KRB5_REALM_UNKNOWN:
                return format_to(ctx.out(), "KRB5_REALM_UNKNOWN");

            case krb5_minor::KRB5_SERVICE_UNKNOWN:
                return format_to(ctx.out(), "KRB5_SERVICE_UNKNOWN");

            case krb5_minor::KRB5_KDC_UNREACH:
                return format_to(ctx.out(), "KRB5_KDC_UNREACH");

            case krb5_minor::KRB5_NO_LOCALNAME:
                return format_to(ctx.out(), "KRB5_NO_LOCALNAME");

            case krb5_minor::KRB5_MUTUAL_FAILED:
                return format_to(ctx.out(), "KRB5_MUTUAL_FAILED");

            case krb5_minor::KRB5_RC_TYPE_EXISTS:
                return format_to(ctx.out(), "KRB5_RC_TYPE_EXISTS");

            case krb5_minor::KRB5_RC_MALLOC:
                return format_to(ctx.out(), "KRB5_RC_MALLOC");

            case krb5_minor::KRB5_RC_TYPE_NOTFOUND:
                return format_to(ctx.out(), "KRB5_RC_TYPE_NOTFOUND");

            case krb5_minor::KRB5_RC_UNKNOWN:
                return format_to(ctx.out(), "KRB5_RC_UNKNOWN");

            case krb5_minor::KRB5_RC_REPLAY:
                return format_to(ctx.out(), "KRB5_RC_REPLAY");

            case krb5_minor::KRB5_RC_IO:
                return format_to(ctx.out(), "KRB5_RC_IO");

            case krb5_minor::KRB5_RC_NOIO:
                return format_to(ctx.out(), "KRB5_RC_NOIO");

            case krb5_minor::KRB5_RC_PARSE:
                return format_to(ctx.out(), "KRB5_RC_PARSE");

            case krb5_minor::KRB5_RC_IO_EOF:
                return format_to(ctx.out(), "KRB5_RC_IO_EOF");

            case krb5_minor::KRB5_RC_IO_MALLOC:
                return format_to(ctx.out(), "KRB5_RC_IO_MALLOC");

            case krb5_minor::KRB5_RC_IO_PERM:
                return format_to(ctx.out(), "KRB5_RC_IO_PERM");

            case krb5_minor::KRB5_RC_IO_IO:
                return format_to(ctx.out(), "KRB5_RC_IO_IO");

            case krb5_minor::KRB5_RC_IO_UNKNOWN:
                return format_to(ctx.out(), "KRB5_RC_IO_UNKNOWN");

            case krb5_minor::KRB5_RC_IO_SPACE:
                return format_to(ctx.out(), "KRB5_RC_IO_SPACE");

            case krb5_minor::KRB5_TRANS_CANTOPEN:
                return format_to(ctx.out(), "KRB5_TRANS_CANTOPEN");

            case krb5_minor::KRB5_TRANS_BADFORMAT:
                return format_to(ctx.out(), "KRB5_TRANS_BADFORMAT");

            case krb5_minor::KRB5_LNAME_CANTOPEN:
                return format_to(ctx.out(), "KRB5_LNAME_CANTOPEN");

            case krb5_minor::KRB5_LNAME_NOTRANS:
                return format_to(ctx.out(), "KRB5_LNAME_NOTRANS");

            case krb5_minor::KRB5_LNAME_BADFORMAT:
                return format_to(ctx.out(), "KRB5_LNAME_BADFORMAT");

            case krb5_minor::KRB5_CRYPTO_INTERNAL:
                return format_to(ctx.out(), "KRB5_CRYPTO_INTERNAL");

            case krb5_minor::KRB5_KT_BADNAME:
                return format_to(ctx.out(), "KRB5_KT_BADNAME");

            case krb5_minor::KRB5_KT_UNKNOWN_TYPE:
                return format_to(ctx.out(), "KRB5_KT_UNKNOWN_TYPE");

            case krb5_minor::KRB5_KT_NOTFOUND:
                return format_to(ctx.out(), "KRB5_KT_NOTFOUND");

            case krb5_minor::KRB5_KT_END:
                return format_to(ctx.out(), "KRB5_KT_END");

            case krb5_minor::KRB5_KT_NOWRITE:
                return format_to(ctx.out(), "KRB5_KT_NOWRITE");

            case krb5_minor::KRB5_KT_IOERR:
                return format_to(ctx.out(), "KRB5_KT_IOERR");

            case krb5_minor::KRB5_NO_TKT_IN_RLM:
                return format_to(ctx.out(), "KRB5_NO_TKT_IN_RLM");

            case krb5_minor::KRB5DES_BAD_KEYPAR:
                return format_to(ctx.out(), "KRB5DES_BAD_KEYPAR");

            case krb5_minor::KRB5DES_WEAK_KEY:
                return format_to(ctx.out(), "KRB5DES_WEAK_KEY");

            case krb5_minor::KRB5_BAD_ENCTYPE:
                return format_to(ctx.out(), "KRB5_BAD_ENCTYPE");

            case krb5_minor::KRB5_BAD_KEYSIZE:
                return format_to(ctx.out(), "KRB5_BAD_KEYSIZE");

            case krb5_minor::KRB5_BAD_MSIZE:
                return format_to(ctx.out(), "KRB5_BAD_MSIZE");

            case krb5_minor::KRB5_CC_TYPE_EXISTS:
                return format_to(ctx.out(), "KRB5_CC_TYPE_EXISTS");

            case krb5_minor::KRB5_KT_TYPE_EXISTS:
                return format_to(ctx.out(), "KRB5_KT_TYPE_EXISTS");

            case krb5_minor::KRB5_CC_IO:
                return format_to(ctx.out(), "KRB5_CC_IO");

            case krb5_minor::KRB5_FCC_PERM:
                return format_to(ctx.out(), "KRB5_FCC_PERM");

            case krb5_minor::KRB5_FCC_NOFILE:
                return format_to(ctx.out(), "KRB5_FCC_NOFILE");

            case krb5_minor::KRB5_FCC_INTERNAL:
                return format_to(ctx.out(), "KRB5_FCC_INTERNAL");

            case krb5_minor::KRB5_CC_WRITE:
                return format_to(ctx.out(), "KRB5_CC_WRITE");

            case krb5_minor::KRB5_CC_NOMEM:
                return format_to(ctx.out(), "KRB5_CC_NOMEM");

            case krb5_minor::KRB5_CC_FORMAT:
                return format_to(ctx.out(), "KRB5_CC_FORMAT");

            case krb5_minor::KRB5_CC_NOT_KTYPE:
                return format_to(ctx.out(), "KRB5_CC_NOT_KTYPE");

            case krb5_minor::KRB5_INVALID_FLAGS:
                return format_to(ctx.out(), "KRB5_INVALID_FLAGS");

            case krb5_minor::KRB5_NO_2ND_TKT:
                return format_to(ctx.out(), "KRB5_NO_2ND_TKT");

            case krb5_minor::KRB5_NOCREDS_SUPPLIED:
                return format_to(ctx.out(), "KRB5_NOCREDS_SUPPLIED");

            case krb5_minor::KRB5_SENDAUTH_BADAUTHVERS:
                return format_to(ctx.out(), "KRB5_SENDAUTH_BADAUTHVERS");

            case krb5_minor::KRB5_SENDAUTH_BADAPPLVERS:
                return format_to(ctx.out(), "KRB5_SENDAUTH_BADAPPLVERS");

            case krb5_minor::KRB5_SENDAUTH_BADRESPONSE:
                return format_to(ctx.out(), "KRB5_SENDAUTH_BADRESPONSE");

            case krb5_minor::KRB5_SENDAUTH_REJECTED:
                return format_to(ctx.out(), "KRB5_SENDAUTH_REJECTED");

            case krb5_minor::KRB5_PREAUTH_BAD_TYPE:
                return format_to(ctx.out(), "KRB5_PREAUTH_BAD_TYPE");

            case krb5_minor::KRB5_PREAUTH_NO_KEY:
                return format_to(ctx.out(), "KRB5_PREAUTH_NO_KEY");

            case krb5_minor::KRB5_PREAUTH_FAILED:
                return format_to(ctx.out(), "KRB5_PREAUTH_FAILED");

            case krb5_minor::KRB5_RCACHE_BADVNO:
                return format_to(ctx.out(), "KRB5_RCACHE_BADVNO");

            case krb5_minor::KRB5_CCACHE_BADVNO:
                return format_to(ctx.out(), "KRB5_CCACHE_BADVNO");

            case krb5_minor::KRB5_KEYTAB_BADVNO:
                return format_to(ctx.out(), "KRB5_KEYTAB_BADVNO");

            case krb5_minor::KRB5_PROG_ATYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5_PROG_ATYPE_NOSUPP");

            case krb5_minor::KRB5_RC_REQUIRED:
                return format_to(ctx.out(), "KRB5_RC_REQUIRED");

            case krb5_minor::KRB5_ERR_BAD_HOSTNAME:
                return format_to(ctx.out(), "KRB5_ERR_BAD_HOSTNAME");

            case krb5_minor::KRB5_ERR_HOST_REALM_UNKNOWN:
                return format_to(ctx.out(), "KRB5_ERR_HOST_REALM_UNKNOWN");

            case krb5_minor::KRB5_SNAME_UNSUPP_NAMETYPE:
                return format_to(ctx.out(), "KRB5_SNAME_UNSUPP_NAMETYPE");

            case krb5_minor::KRB5KRB_AP_ERR_V4_REPLY:
                return format_to(ctx.out(), "KRB5KRB_AP_ERR_V4_REPLY");

            case krb5_minor::KRB5_REALM_CANT_RESOLVE:
                return format_to(ctx.out(), "KRB5_REALM_CANT_RESOLVE");

            case krb5_minor::KRB5_TKT_NOT_FORWARDABLE:
                return format_to(ctx.out(), "KRB5_TKT_NOT_FORWARDABLE");

            case krb5_minor::KRB5_FWD_BAD_PRINCIPAL:
                return format_to(ctx.out(), "KRB5_FWD_BAD_PRINCIPAL");

            case krb5_minor::KRB5_GET_IN_TKT_LOOP:
                return format_to(ctx.out(), "KRB5_GET_IN_TKT_LOOP");

            case krb5_minor::KRB5_CONFIG_NODEFREALM:
                return format_to(ctx.out(), "KRB5_CONFIG_NODEFREALM");

            case krb5_minor::KRB5_SAM_UNSUPPORTED:
                return format_to(ctx.out(), "KRB5_SAM_UNSUPPORTED");

            case krb5_minor::KRB5_SAM_INVALID_ETYPE:
                return format_to(ctx.out(), "KRB5_SAM_INVALID_ETYPE");

            case krb5_minor::KRB5_SAM_NO_CHECKSUM:
                return format_to(ctx.out(), "KRB5_SAM_NO_CHECKSUM");

            case krb5_minor::KRB5_SAM_BAD_CHECKSUM:
                return format_to(ctx.out(), "KRB5_SAM_BAD_CHECKSUM");

            case krb5_minor::KRB5_KT_NAME_TOOLONG:
                return format_to(ctx.out(), "KRB5_KT_NAME_TOOLONG");

            case krb5_minor::KRB5_KT_KVNONOTFOUND:
                return format_to(ctx.out(), "KRB5_KT_KVNONOTFOUND");

            case krb5_minor::KRB5_APPL_EXPIRED:
                return format_to(ctx.out(), "KRB5_APPL_EXPIRED");

            case krb5_minor::KRB5_LIB_EXPIRED:
                return format_to(ctx.out(), "KRB5_LIB_EXPIRED");

            case krb5_minor::KRB5_CHPW_PWDNULL:
                return format_to(ctx.out(), "KRB5_CHPW_PWDNULL");

            case krb5_minor::KRB5_CHPW_FAIL:
                return format_to(ctx.out(), "KRB5_CHPW_FAIL");

            case krb5_minor::KRB5_KT_FORMAT:
                return format_to(ctx.out(), "KRB5_KT_FORMAT");

            case krb5_minor::KRB5_NOPERM_ETYPE:
                return format_to(ctx.out(), "KRB5_NOPERM_ETYPE");

            case krb5_minor::KRB5_CONFIG_ETYPE_NOSUPP:
                return format_to(ctx.out(), "KRB5_CONFIG_ETYPE_NOSUPP");

            case krb5_minor::KRB5_OBSOLETE_FN:
                return format_to(ctx.out(), "KRB5_OBSOLETE_FN");

            case krb5_minor::KRB5_EAI_FAIL:
                return format_to(ctx.out(), "KRB5_EAI_FAIL");

            case krb5_minor::KRB5_EAI_NODATA:
                return format_to(ctx.out(), "KRB5_EAI_NODATA");

            case krb5_minor::KRB5_EAI_NONAME:
                return format_to(ctx.out(), "KRB5_EAI_NONAME");

            case krb5_minor::KRB5_EAI_SERVICE:
                return format_to(ctx.out(), "KRB5_EAI_SERVICE");

            case krb5_minor::KRB5_ERR_NUMERIC_REALM:
                return format_to(ctx.out(), "KRB5_ERR_NUMERIC_REALM");

            case krb5_minor::KRB5_ERR_BAD_S2K_PARAMS:
                return format_to(ctx.out(), "KRB5_ERR_BAD_S2K_PARAMS");

            case krb5_minor::KRB5_ERR_NO_SERVICE:
                return format_to(ctx.out(), "KRB5_ERR_NO_SERVICE");

            case krb5_minor::KRB5_CC_READONLY:
                return format_to(ctx.out(), "KRB5_CC_READONLY");

            case krb5_minor::KRB5_CC_NOSUPP:
                return format_to(ctx.out(), "KRB5_CC_NOSUPP");

            case krb5_minor::KRB5_DELTAT_BADFORMAT:
                return format_to(ctx.out(), "KRB5_DELTAT_BADFORMAT");

            case krb5_minor::KRB5_PLUGIN_NO_HANDLE:
                return format_to(ctx.out(), "KRB5_PLUGIN_NO_HANDLE");

            case krb5_minor::KRB5_PLUGIN_OP_NOTSUPP:
                return format_to(ctx.out(), "KRB5_PLUGIN_OP_NOTSUPP");

            case krb5_minor::KRB5_ERR_INVALID_UTF8:
                return format_to(ctx.out(), "KRB5_ERR_INVALID_UTF8");

            case krb5_minor::KRB5_ERR_FAST_REQUIRED:
                return format_to(ctx.out(), "KRB5_ERR_FAST_REQUIRED");

            case krb5_minor::KRB5_LOCAL_ADDR_REQUIRED:
                return format_to(ctx.out(), "KRB5_LOCAL_ADDR_REQUIRED");

            case krb5_minor::KRB5_REMOTE_ADDR_REQUIRED:
                return format_to(ctx.out(), "KRB5_REMOTE_ADDR_REQUIRED");

            case krb5_minor::KRB5_TRACE_NOSUPP:
                return format_to(ctx.out(), "KRB5_TRACE_NOSUPP");

            default:
                return format_to(ctx.out(), "{}", (int32_t)t);
        }
    }
};

class gss_error : public exception {
public:
    gss_error(const string& func, OM_uint32 major, OM_uint32 minor) {
        OM_uint32 message_context = 0;
        OM_uint32 min_status;
        gss_buffer_desc status_string;
        bool first = true;

        msg = format("{} failed (minor {}): ", func, (enum krb5_minor)minor);

        do {
            gss_display_status(&min_status, major, GSS_C_GSS_CODE, GSS_C_NO_OID,
                               &message_context, &status_string);

            if (!first)
                msg += "; ";

            msg += string((char*)status_string.value, status_string.length);

            gss_release_buffer(&min_status, &status_string);
            first = false;
        } while (message_context != 0);
    }

    const char* what() const noexcept {
        return msg.c_str();
    }

private:
    string msg;
};
#endif

#ifdef _WIN32
static string wsa_error_to_string(int err) {
    switch (err) {
        case WSA_INVALID_HANDLE: return "WSA_INVALID_HANDLE";
        case WSA_NOT_ENOUGH_MEMORY: return "WSA_NOT_ENOUGH_MEMORY";
        case WSA_INVALID_PARAMETER: return "WSA_INVALID_PARAMETER";
        case WSA_OPERATION_ABORTED: return "WSA_OPERATION_ABORTED";
        case WSA_IO_INCOMPLETE: return "WSA_IO_INCOMPLETE";
        case WSA_IO_PENDING: return "WSA_IO_PENDING";
        case WSAEINTR: return "WSAEINTR";
        case WSAEBADF: return "WSAEBADF";
        case WSAEACCES: return "WSAEACCES";
        case WSAEFAULT: return "WSAEFAULT";
        case WSAEINVAL: return "WSAEINVAL";
        case WSAEMFILE: return "WSAEMFILE";
        case WSAEWOULDBLOCK: return "WSAEWOULDBLOCK";
        case WSAEINPROGRESS: return "WSAEINPROGRESS";
        case WSAEALREADY: return "WSAEALREADY";
        case WSAENOTSOCK: return "WSAENOTSOCK";
        case WSAEDESTADDRREQ: return "WSAEDESTADDRREQ";
        case WSAEMSGSIZE: return "WSAEMSGSIZE";
        case WSAEPROTOTYPE: return "WSAEPROTOTYPE";
        case WSAENOPROTOOPT: return "WSAENOPROTOOPT";
        case WSAEPROTONOSUPPORT: return "WSAEPROTONOSUPPORT";
        case WSAESOCKTNOSUPPORT: return "WSAESOCKTNOSUPPORT";
        case WSAEOPNOTSUPP: return "WSAEOPNOTSUPP";
        case WSAEPFNOSUPPORT: return "WSAEPFNOSUPPORT";
        case WSAEAFNOSUPPORT: return "WSAEAFNOSUPPORT";
        case WSAEADDRINUSE: return "WSAEADDRINUSE";
        case WSAEADDRNOTAVAIL: return "WSAEADDRNOTAVAIL";
        case WSAENETDOWN: return "WSAENETDOWN";
        case WSAENETUNREACH: return "WSAENETUNREACH";
        case WSAENETRESET: return "WSAENETRESET";
        case WSAECONNABORTED: return "WSAECONNABORTED";
        case WSAECONNRESET: return "WSAECONNRESET";
        case WSAENOBUFS: return "WSAENOBUFS";
        case WSAEISCONN: return "WSAEISCONN";
        case WSAENOTCONN: return "WSAENOTCONN";
        case WSAESHUTDOWN: return "WSAESHUTDOWN";
        case WSAETOOMANYREFS: return "WSAETOOMANYREFS";
        case WSAETIMEDOUT: return "WSAETIMEDOUT";
        case WSAECONNREFUSED: return "WSAECONNREFUSED";
        case WSAELOOP: return "WSAELOOP";
        case WSAENAMETOOLONG: return "WSAENAMETOOLONG";
        case WSAEHOSTDOWN: return "WSAEHOSTDOWN";
        case WSAEHOSTUNREACH: return "WSAEHOSTUNREACH";
        case WSAENOTEMPTY: return "WSAENOTEMPTY";
        case WSAEPROCLIM: return "WSAEPROCLIM";
        case WSAEUSERS: return "WSAEUSERS";
        case WSAEDQUOT: return "WSAEDQUOT";
        case WSAESTALE: return "WSAESTALE";
        case WSAEREMOTE: return "WSAEREMOTE";
        case WSASYSNOTREADY: return "WSASYSNOTREADY";
        case WSAVERNOTSUPPORTED: return "WSAVERNOTSUPPORTED";
        case WSANOTINITIALISED: return "WSANOTINITIALISED";
        case WSAEDISCON: return "WSAEDISCON";
        case WSAENOMORE: return "WSAENOMORE";
        case WSAECANCELLED: return "WSAECANCELLED";
        case WSAEINVALIDPROCTABLE: return "WSAEINVALIDPROCTABLE";
        case WSAEINVALIDPROVIDER: return "WSAEINVALIDPROVIDER";
        case WSAEPROVIDERFAILEDINIT: return "WSAEPROVIDERFAILEDINIT";
        case WSASYSCALLFAILURE: return "WSASYSCALLFAILURE";
        case WSASERVICE_NOT_FOUND: return "WSASERVICE_NOT_FOUND";
        case WSATYPE_NOT_FOUND: return "WSATYPE_NOT_FOUND";
        case WSA_E_NO_MORE: return "WSA_E_NO_MORE";
        case WSA_E_CANCELLED: return "WSA_E_CANCELLED";
        case WSAEREFUSED: return "WSAEREFUSED";
        case WSAHOST_NOT_FOUND: return "WSAHOST_NOT_FOUND";
        case WSATRY_AGAIN: return "WSATRY_AGAIN";
        case WSANO_RECOVERY: return "WSANO_RECOVERY";
        case WSANO_DATA: return "WSANO_DATA";
        case WSA_QOS_RECEIVERS: return "WSA_QOS_RECEIVERS";
        case WSA_QOS_SENDERS: return "WSA_QOS_SENDERS";
        case WSA_QOS_NO_SENDERS: return "WSA_QOS_NO_SENDERS";
        case WSA_QOS_NO_RECEIVERS: return "WSA_QOS_NO_RECEIVERS";
        case WSA_QOS_REQUEST_CONFIRMED: return "WSA_QOS_REQUEST_CONFIRMED";
        case WSA_QOS_ADMISSION_FAILURE: return "WSA_QOS_ADMISSION_FAILURE";
        case WSA_QOS_POLICY_FAILURE: return "WSA_QOS_POLICY_FAILURE";
        case WSA_QOS_BAD_STYLE: return "WSA_QOS_BAD_STYLE";
        case WSA_QOS_BAD_OBJECT: return "WSA_QOS_BAD_OBJECT";
        case WSA_QOS_TRAFFIC_CTRL_ERROR: return "WSA_QOS_TRAFFIC_CTRL_ERROR";
        case WSA_QOS_GENERIC_ERROR: return "WSA_QOS_GENERIC_ERROR";
        case WSA_QOS_ESERVICETYPE: return "WSA_QOS_ESERVICETYPE";
        case WSA_QOS_EFLOWSPEC: return "WSA_QOS_EFLOWSPEC";
        case WSA_QOS_EPROVSPECBUF: return "WSA_QOS_EPROVSPECBUF";
        case WSA_QOS_EFILTERSTYLE: return "WSA_QOS_EFILTERSTYLE";
        case WSA_QOS_EFILTERTYPE: return "WSA_QOS_EFILTERTYPE";
        case WSA_QOS_EFILTERCOUNT: return "WSA_QOS_EFILTERCOUNT";
        case WSA_QOS_EOBJLENGTH: return "WSA_QOS_EOBJLENGTH";
        case WSA_QOS_EFLOWCOUNT: return "WSA_QOS_EFLOWCOUNT";
        case WSA_QOS_EUNKOWNPSOBJ: return "WSA_QOS_EUNKOWNPSOBJ";
        case WSA_QOS_EPOLICYOBJ: return "WSA_QOS_EPOLICYOBJ";
        case WSA_QOS_EFLOWDESC: return "WSA_QOS_EFLOWDESC";
        case WSA_QOS_EPSFLOWSPEC: return "WSA_QOS_EPSFLOWSPEC";
        case WSA_QOS_EPSFILTERSPEC: return "WSA_QOS_EPSFILTERSPEC";
        case WSA_QOS_ESDMODEOBJ: return "WSA_QOS_ESDMODEOBJ";
        case WSA_QOS_ESHAPERATEOBJ: return "WSA_QOS_ESHAPERATEOBJ";
        case WSA_QOS_RESERVED_PETYPE: return "WSA_QOS_RESERVED_PETYPE";
        default: return to_string(err);
    }
}
#else
static string errno_to_string(int err) {
    switch (err) {
        case E2BIG: return "E2BIG";
        case EACCES: return "EACCES";
        case EADDRINUSE: return "EADDRINUSE";
        case EADDRNOTAVAIL: return "EADDRNOTAVAIL";
        case EAFNOSUPPORT: return "EAFNOSUPPORT";
        case EALREADY: return "EALREADY";
        case EBADE: return "EBADE";
        case EBADF: return "EBADF";
        case EBADFD: return "EBADFD";
        case EBADMSG: return "EBADMSG";
        case EBADR: return "EBADR";
        case EBADRQC: return "EBADRQC";
        case EBADSLT: return "EBADSLT";
        case EBUSY: return "EBUSY";
        case ECANCELED: return "ECANCELED";
        case ECHILD: return "ECHILD";
        case ECHRNG: return "ECHRNG";
        case ECOMM: return "ECOMM";
        case ECONNABORTED: return "ECONNABORTED";
        case ECONNREFUSED: return "ECONNREFUSED";
        case ECONNRESET: return "ECONNRESET";
        case EDEADLOCK: return "EDEADLOCK";
        case EDESTADDRREQ: return "EDESTADDRREQ";
        case EDOM: return "EDOM";
        case EDQUOT: return "EDQUOT";
        case EEXIST: return "EEXIST";
        case EFAULT: return "EFAULT";
        case EFBIG: return "EFBIG";
        case EHOSTDOWN: return "EHOSTDOWN";
        case EHOSTUNREACH: return "EHOSTUNREACH";
        case EHWPOISON: return "EHWPOISON";
        case EIDRM: return "EIDRM";
        case EILSEQ: return "EILSEQ";
        case EINPROGRESS: return "EINPROGRESS";
        case EINTR: return "EINTR";
        case EINVAL: return "EINVAL";
        case EIO: return "EIO";
        case EISCONN: return "EISCONN";
        case EISDIR: return "EISDIR";
        case EISNAM: return "EISNAM";
        case EKEYEXPIRED: return "EKEYEXPIRED";
        case EKEYREJECTED: return "EKEYREJECTED";
        case EKEYREVOKED: return "EKEYREVOKED";
        case EL2HLT: return "EL2HLT";
        case EL2NSYNC: return "EL2NSYNC";
        case EL3HLT: return "EL3HLT";
        case EL3RST: return "EL3RST";
        case ELIBACC: return "ELIBACC";
        case ELIBBAD: return "ELIBBAD";
        case ELIBMAX: return "ELIBMAX";
        case ELIBSCN: return "ELIBSCN";
        case ELIBEXEC: return "ELIBEXEC";
        case ELOOP: return "ELOOP";
        case EMEDIUMTYPE: return "EMEDIUMTYPE";
        case EMFILE: return "EMFILE";
        case EMLINK: return "EMLINK";
        case EMSGSIZE: return "EMSGSIZE";
        case EMULTIHOP: return "EMULTIHOP";
        case ENAMETOOLONG: return "ENAMETOOLONG";
        case ENETDOWN: return "ENETDOWN";
        case ENETRESET: return "ENETRESET";
        case ENETUNREACH: return "ENETUNREACH";
        case ENFILE: return "ENFILE";
        case ENOANO: return "ENOANO";
        case ENOBUFS: return "ENOBUFS";
        case ENODATA: return "ENODATA";
        case ENODEV: return "ENODEV";
        case ENOENT: return "ENOENT";
        case ENOEXEC: return "ENOEXEC";
        case ENOKEY: return "ENOKEY";
        case ENOLCK: return "ENOLCK";
        case ENOLINK: return "ENOLINK";
        case ENOMEDIUM: return "ENOMEDIUM";
        case ENOMEM: return "ENOMEM";
        case ENOMSG: return "ENOMSG";
        case ENONET: return "ENONET";
        case ENOPKG: return "ENOPKG";
        case ENOPROTOOPT: return "ENOPROTOOPT";
        case ENOSPC: return "ENOSPC";
        case ENOSR: return "ENOSR";
        case ENOSTR: return "ENOSTR";
        case ENOSYS: return "ENOSYS";
        case ENOTBLK: return "ENOTBLK";
        case ENOTCONN: return "ENOTCONN";
        case ENOTDIR: return "ENOTDIR";
        case ENOTEMPTY: return "ENOTEMPTY";
        case ENOTRECOVERABLE: return "ENOTRECOVERABLE";
        case ENOTSOCK: return "ENOTSOCK";
        case ENOTSUP: return "ENOTSUP";
        case ENOTTY: return "ENOTTY";
        case ENOTUNIQ: return "ENOTUNIQ";
        case ENXIO: return "ENXIO";
        case EOVERFLOW: return "EOVERFLOW";
        case EOWNERDEAD: return "EOWNERDEAD";
        case EPERM: return "EPERM";
        case EPFNOSUPPORT: return "EPFNOSUPPORT";
        case EPIPE: return "EPIPE";
        case EPROTO: return "EPROTO";
        case EPROTONOSUPPORT: return "EPROTONOSUPPORT";
        case EPROTOTYPE: return "EPROTOTYPE";
        case ERANGE: return "ERANGE";
        case EREMCHG: return "EREMCHG";
        case EREMOTE: return "EREMOTE";
        case EREMOTEIO: return "EREMOTEIO";
        case ERESTART: return "ERESTART";
        case ERFKILL: return "ERFKILL";
        case EROFS: return "EROFS";
        case ESHUTDOWN: return "ESHUTDOWN";
        case ESPIPE: return "ESPIPE";
        case ESOCKTNOSUPPORT: return "ESOCKTNOSUPPORT";
        case ESRCH: return "ESRCH";
        case ESTALE: return "ESTALE";
        case ESTRPIPE: return "ESTRPIPE";
        case ETIME: return "ETIME";
        case ETIMEDOUT: return "ETIMEDOUT";
        case ETOOMANYREFS: return "ETOOMANYREFS";
        case ETXTBSY: return "ETXTBSY";
        case EUCLEAN: return "EUCLEAN";
        case EUNATCH: return "EUNATCH";
        case EUSERS: return "EUSERS";
        case EWOULDBLOCK: return "EWOULDBLOCK";
        case EXDEV: return "EXDEV";
        case EXFULL: return "EXFULL";
        default: return to_string(err);
    }
}
#endif

#ifdef _WIN32

event::event() {
    h.reset(CreateEventW(nullptr, false, false, nullptr));

    if (!h)
        throw last_error("CreateEvent", GetLastError());
}

void event::reset() {
    ResetEvent(h.get());
}

void event::set() {
    SetEvent(h.get());
}

#else

event::event() {
    h.reset(eventfd(0, EFD_CLOEXEC));

    if (h.get() == -1)
        throw formatted_error("eventfd failed (error {})", errno_to_string(errno));
}

void event::reset() {
    uint64_t num;

    do {
        if (read(h.get(), &num, sizeof(num)) == sizeof(num))
            break;

        if (errno == EINTR)
            continue;

        throw formatted_error("read failed (error {})", errno_to_string(errno));
    } while (true);
}

void event::set() {
    uint64_t num = 1;

    do {
        if (write(h.get(), &num, sizeof(num)) == sizeof(num))
            break;

        if (errno == EINTR)
            continue;

        throw formatted_error("write failed (error {})", errno_to_string(errno));
    } while (true);
}

#endif

static void name_thread(string_view name) {
#ifdef _WIN32
    if (auto h = LoadLibraryW(L"kernelbase.dll")) {
        HRESULT (WINAPI *_SetThreadDescription)(HANDLE hThread, PCWSTR lpThreadDescription);

        _SetThreadDescription = (decltype(_SetThreadDescription))(void(*)(void))GetProcAddress(h, "SetThreadDescription");

        if (_SetThreadDescription) {
            auto namew = tds::utf8_to_utf16(name);
            _SetThreadDescription(GetCurrentThread(), (WCHAR*)namew.c_str());
        }
    }
#else
    auto fn = format("/proc/self/task/{}/comm", gettid());

    unique_handle h{open(fn.c_str(), O_WRONLY)};

    if (h.get() > 0)
        write(h.get(), name.data(), name.size());
#endif
}

void handle_nbcrow(span<const uint8_t>& sp, const vector<tds::column>& cols, list<vector<pair<tds::value_data_t, bool>>>& rows) {
    if (cols.empty())
        return;

    rows.emplace_back();
    auto& row = rows.back();

    row.resize(cols.size());

    auto bitset_length = (cols.size() + 7) / 8;

    if (sp.size() < bitset_length)
        throw formatted_error("Short NBCROW message ({} bytes, expected at least {}).", sp.size(), bitset_length);

    auto bitset = sp.subspan(0, bitset_length);
    auto bsv = bitset[0];

    sp = sp.subspan(bitset_length);

    for (unsigned int i = 0; i < row.size(); i++) {
        auto& col = row[i];

        if (i != 0) {
            if ((i & 7) == 0) {
                bitset = bitset.subspan(1);
                bsv = bitset[0];
            } else
                bsv >>= 1;
        }

        if (bsv & 1) // NULL
            get<1>(col) = true;
        else
            handle_row_col(get<0>(col), get<1>(col), cols[i].type, cols[i].max_length, sp);
    }
}

#ifdef _WIN32
class sspi_handle {
public:
    sspi_handle() {
        SECURITY_STATUS sec_status;
        TimeStamp timestamp;

        sec_status = AcquireCredentialsHandleW(nullptr, (SEC_WCHAR*)L"Negotiate", SECPKG_CRED_OUTBOUND, nullptr,
                                                nullptr, nullptr, nullptr, &cred_handle, &timestamp);
        if (FAILED(sec_status))
            throw formatted_error("AcquireCredentialsHandle returned {}", (enum sec_error)sec_status);
    }

    ~sspi_handle() {
        if (ctx_handle_set)
            DeleteSecurityContext(&ctx_handle);

        FreeCredentialsHandle(&cred_handle);
    }

    SECURITY_STATUS init_security_context(const char16_t* target_name, uint32_t context_req, uint32_t target_data_rep,
                                            PSecBufferDesc input, PSecBufferDesc output, uint32_t* context_attr,
                                            PTimeStamp timestamp) {
        SECURITY_STATUS sec_status;

        sec_status = InitializeSecurityContextW(&cred_handle, nullptr, (SEC_WCHAR*)target_name, context_req, 0,
                                                target_data_rep, input, 0, &ctx_handle, output,
                                                (ULONG*)context_attr, timestamp);

        if (FAILED(sec_status))
            throw formatted_error("InitializeSecurityContext returned {}", (enum sec_error)sec_status);

        ctx_handle_set = true;

        return sec_status;
    }

    CredHandle cred_handle = {(ULONG_PTR)-1, (ULONG_PTR)-1};
    CtxtHandle ctx_handle;
    bool ctx_handle_set = false;
};
#endif

#if !defined(_WIN32) && defined(HAVE_GSSAPI)
class gss_name_deleter {
public:
    typedef gss_name_t pointer;

    void operator()(gss_name_t n) {
        OM_uint32 status;

        gss_release_name(&status, &n);
    }
};
#endif

static void send_login_msg2(uint32_t tds_version, uint32_t packet_size, uint32_t client_version, uint32_t client_pid,
                            uint32_t connexion_id, uint8_t option_flags1, uint8_t option_flags2, bool read_only_intent,
                            uint8_t option_flags3, uint32_t collation, u16string_view client_name,
                            u16string_view username, u16string_view password, u16string_view app_name,
                            u16string_view server_name, u16string_view interface_library,
                            u16string_view locale, u16string_view database, span<const uint8_t> sspi,
                            u16string_view attach_db, u16string_view new_password, tds::sendable auto& sess) {
    uint32_t length;
    uint16_t off;

    static const vector<string> features = {
        "\x0a\x01\x00\x00\x00\x01"s // UTF-8 support
    };

    length = sizeof(tds_login_msg);
    length += (uint32_t)(client_name.length() * sizeof(char16_t));
    length += (uint32_t)(username.length() * sizeof(char16_t));
    length += (uint32_t)(password.length() * sizeof(char16_t));
    length += (uint32_t)(app_name.length() * sizeof(char16_t));
    length += (uint32_t)(server_name.length() * sizeof(char16_t));
    length += (uint32_t)(interface_library.length() * sizeof(char16_t));
    length += (uint32_t)(locale.length() * sizeof(char16_t));
    length += (uint32_t)(database.length() * sizeof(char16_t));
    length += (uint32_t)sspi.size();

    length += sizeof(uint32_t);
    for (const auto& f : features) {
        length += (uint32_t)f.length();
    }
    length += sizeof(uint8_t);

    vector<uint8_t> payload;

    payload.resize(length);

    auto& msg = *(tds_login_msg*)payload.data();

    msg.length = length;
    msg.tds_version = tds_version;
    msg.packet_size = packet_size;
    msg.client_version = client_version;
    msg.client_pid = client_pid;
    msg.connexion_id = connexion_id;
    msg.option_flags1 = option_flags1;
    msg.option_flags2 = option_flags2 | (uint8_t)(!sspi.empty() ? 0x80 : 0);
    msg.sql_type_flags.read_only_intent = read_only_intent ? 1 : 0;
    msg.option_flags3 = option_flags3 | 0x10;
    msg.timezone = 0;
    msg.collation = collation;

    off = sizeof(tds_login_msg);

    msg.client_name_offset = off;

    if (!client_name.empty()) {
        msg.client_name_length = (uint16_t)client_name.length();
        memcpy((uint8_t*)&msg + msg.client_name_offset, client_name.data(),
                client_name.length() * sizeof(char16_t));

        off += (uint16_t)(client_name.length() * sizeof(char16_t));
    } else
        msg.client_name_length = 0;

    msg.username_offset = off;

    if (!username.empty()) {
        msg.username_length = (uint16_t)username.length();
        memcpy((uint8_t*)&msg + msg.username_offset, username.data(),
                username.length() * sizeof(char16_t));

        off += (uint16_t)(username.length() * sizeof(char16_t));
    } else
        msg.username_length = 0;

    msg.password_offset = off;

    if (!password.empty()) {
        msg.password_length = (uint16_t)password.length();

        auto pw_dest = (uint8_t*)&msg + msg.password_offset;
        auto pw_src = (uint8_t*)password.data();

        for (unsigned int i = 0; i < password.length() * sizeof(char16_t); i++) {
            uint8_t c = *pw_src;

            c = (uint8_t)(((c & 0xf) << 4) | (c >> 4));
            c ^= 0xa5;

            *pw_dest = c;

            pw_src++;
            pw_dest++;
        }

        off += (uint16_t)(password.length() * sizeof(char16_t));
    } else
        msg.password_length = 0;

    msg.app_name_offset = off;

    if (!app_name.empty()) {
        msg.app_name_length = (uint16_t)app_name.length();
        memcpy((uint8_t*)&msg + msg.app_name_offset, app_name.data(),
                app_name.length() * sizeof(char16_t));

        off += (uint16_t)(app_name.length() * sizeof(char16_t));
    } else
        msg.app_name_length = 0;

    msg.server_name_offset = off;

    if (!server_name.empty()) {
        msg.server_name_length = (uint16_t)server_name.length();
        memcpy((uint8_t*)&msg + msg.server_name_offset, server_name.data(),
                server_name.length() * sizeof(char16_t));

        off += (uint16_t)(server_name.length() * sizeof(char16_t));
    } else
        msg.server_name_length = 0;

    msg.interface_library_offset = off;

    if (!interface_library.empty()) {
        msg.interface_library_length = (uint16_t)interface_library.length();
        memcpy((uint8_t*)&msg + msg.interface_library_offset, interface_library.data(),
                interface_library.length() * sizeof(char16_t));

        off += (uint16_t)(interface_library.length() * sizeof(char16_t));
    } else
        msg.interface_library_length = 0;

    msg.locale_offset = off;

    if (!locale.empty()) {
        msg.locale_length = (uint16_t)locale.length();
        memcpy((uint8_t*)&msg + msg.locale_offset, locale.data(),
                locale.length() * sizeof(char16_t));

        off += (uint16_t)(locale.length() * sizeof(char16_t));
    } else
        msg.locale_length = 0;

    msg.database_offset = off;

    if (!database.empty()) {
        msg.database_length = (uint16_t)database.length();
        memcpy((uint8_t*)&msg + msg.database_offset, database.data(),
                database.length() * sizeof(char16_t));

        off += (uint16_t)(database.length() * sizeof(char16_t));
    } else
        msg.database_length = 0;

    // FIXME - set MAC address properly?
    memset(msg.mac_address, 0, 6);

    msg.attach_db_offset = off;

    if (!attach_db.empty()) {
        msg.attach_db_length = (uint16_t)attach_db.length();
        memcpy((uint8_t*)&msg + msg.attach_db_offset, attach_db.data(),
                attach_db.length() * sizeof(char16_t));

        off += (uint16_t)(attach_db.length() * sizeof(char16_t));
    } else
        msg.attach_db_length = 0;

    msg.new_password_offset = off;

    if (!new_password.empty()) {
        msg.new_password_length = (uint16_t)new_password.length();
        memcpy((uint8_t*)&msg + msg.new_password_offset, new_password.data(),
                new_password.length() * sizeof(char16_t));

        off += (uint16_t)(new_password.length() * sizeof(char16_t));
    } else
        msg.new_password_length = 0;

    if (sspi.empty()) {
        msg.sspi_offset = 0;
        msg.sspi_length = 0;
        msg.sspi_long = 0;
    } else {
        msg.sspi_offset = off;

        if (sspi.size() >= numeric_limits<uint16_t>::max()) {
            msg.sspi_length = numeric_limits<uint16_t>::max();
            msg.sspi_long = (uint32_t)sspi.size();
        } else {
            msg.sspi_length = (uint16_t)sspi.size();
            msg.sspi_long = 0;
        }

        memcpy((uint8_t*)&msg + msg.sspi_offset, sspi.data(), sspi.size());

        off += (uint16_t)sspi.size();
    }

    msg.extension_offset = off;
    msg.extension_length = sizeof(uint32_t);

    *(uint32_t*)((uint8_t*)&msg + msg.extension_offset) = off + sizeof(uint32_t);
    off += sizeof(uint32_t);

    for (const auto& f : features) {
        memcpy((uint8_t*)&msg + off, f.data(), f.length());
        off += (uint16_t)f.length();
    }

    *(enum tds_feature*)((uint8_t*)&msg + off) = tds_feature::TERMINATOR;

    sess.send_msg(tds_msg::tds7_login, payload);
}

static void handle_loginack_msg(span<const uint8_t> sp) {
    uint8_t server_name_len;
    uint32_t tds_version;
#ifdef DEBUG_SHOW_MSGS
    uint8_t interf;
    uint32_t server_version;
#endif
    u16string_view server_name;

    if (sp.size() < 10)
        throw runtime_error("Short LOGINACK message.");

    server_name_len = sp[5];

    if (sp.size() < 10 + (server_name_len * sizeof(char16_t)))
        throw runtime_error("Short LOGINACK message.");

#ifdef DEBUG_SHOW_MSGS
    interf = sp[0];
#endif
    tds_version = *(uint32_t*)&sp[1];
    server_name = u16string_view((char16_t*)&sp[6], server_name_len);
#ifdef DEBUG_SHOW_MSGS
    server_version = *(uint32_t*)&sp[6 + (server_name_len * sizeof(char16_t))];
#endif

#ifdef DEBUG_SHOW_MSGS
    while (!server_name.empty() && server_name.back() == 0) {
        server_name = server_name.substr(0, server_name.length() - 1);
    }

    cout << format("LOGINACK: interface = {}, TDS version = {:x}, server = {}, server version = {}.{}.{}\n",
                interf, tds_version, utf16_to_utf8(server_name), server_version & 0xff, (server_version & 0xff00) >> 8,
                ((server_version & 0xff0000) >> 8) | (server_version >> 24));
#endif

    if (tds_version != tds_74_version)
        throw formatted_error("Server not using TDS 7.4. Version was {:x}, expected {:x}.", tds_version, tds_74_version);
}

#ifdef _WIN32
static void send_sspi_msg(CredHandle* cred_handle, CtxtHandle* ctx_handle, const u16string& spn, span<const uint8_t> sspi,
                          tds::encryption_type server_enc, tds::sendable auto& sess) {
    SECURITY_STATUS sec_status;
    TimeStamp timestamp;
    SecBuffer inbufs[2], outbuf;
    SecBufferDesc in, out;
    unsigned long context_attr;
    string ret;

    inbufs[0].cbBuffer = (uint32_t)sspi.size();
    inbufs[0].BufferType = SECBUFFER_TOKEN;
    inbufs[0].pvBuffer = (void*)sspi.data();

    inbufs[1].cbBuffer = 0;
    inbufs[1].BufferType = SECBUFFER_EMPTY;
    inbufs[1].pvBuffer = nullptr;

    in.ulVersion = SECBUFFER_VERSION;
    in.cBuffers = 2;
    in.pBuffers = inbufs;

    outbuf.cbBuffer = 0;
    outbuf.BufferType = SECBUFFER_TOKEN;
    outbuf.pvBuffer = nullptr;

    out.ulVersion = SECBUFFER_VERSION;
    out.cBuffers = 1;
    out.pBuffers = &outbuf;

    sec_status = InitializeSecurityContextW(cred_handle, ctx_handle, (SEC_WCHAR*)spn.c_str(),
                                            ISC_REQ_ALLOCATE_MEMORY, 0, SECURITY_NATIVE_DREP,
                                            &in, 0, ctx_handle, &out, &context_attr, &timestamp);
    if (FAILED(sec_status))
        throw formatted_error("InitializeSecurityContext returned {}", (enum sec_error)sec_status);

    ret = string((char*)outbuf.pvBuffer, outbuf.cbBuffer);

    if (outbuf.pvBuffer)
        FreeContextBuffer(outbuf.pvBuffer);

    if (!ret.empty()) {
        sess.send_msg(tds_msg::sspi, span((uint8_t*)ret.data(), ret.size()),
                      server_enc == tds::encryption_type::ENCRYPT_ON || server_enc == tds::encryption_type::ENCRYPT_REQ);
    }
}
#endif

namespace tds {
#if __cpp_lib_constexpr_string >= 201907L
    static_assert(utf8_to_utf16("hello") == u"hello"); // single bytes
    static_assert(utf8_to_utf16("h\xc3\xa9llo") == u"h\xe9llo"); // 2-byte literal
    static_assert(utf8_to_utf16("h\xe2\x82\xacllo") == u"h\u20acllo"); // 3-byte literal
    static_assert(utf8_to_utf16("h\xf0\x9f\x95\xb4llo") == u"h\U0001f574llo"); // 4-byte literal
    static_assert(utf8_to_utf16("h\xc3llo") == u"h\ufffdllo"); // first byte of 2-byte literal
    static_assert(utf8_to_utf16("h\xe2llo") == u"h\ufffdllo"); // first byte of 3-byte literal
    static_assert(utf8_to_utf16("h\xe2\x82llo") == u"h\ufffd\ufffdllo"); // first two bytes of 3-byte literal
    static_assert(utf8_to_utf16("h\xf0llo") == u"h\ufffdllo"); // first byte of 4-byte literal
    static_assert(utf8_to_utf16("h\xf0\x9fllo") == u"h\ufffd\ufffdllo"); // first two bytes of 4-byte literal
    static_assert(utf8_to_utf16("h\xf0\x9f\x95llo") == u"h\ufffd\ufffd\ufffdllo"); // first three bytes of 4-byte literal
    static_assert(utf8_to_utf16("h\xed\xa0\xbdllo") == u"h\ufffdllo"); // encoded surrogate

#ifndef __clang__ // doesn't work with clang 17
    static_assert(utf16_to_utf8(u"hello") == "hello"); // single bytes
#endif
    // Compiler bug on MSVC 16.10? These work as asserts but not static_asserts
//     static_assert(utf16_to_utf8(u"h\xe9llo") == "h\xc3\xa9llo"); // 2-byte literal
//     static_assert(utf16_to_utf8(u"h\u20acllo") == "h\xe2\x82\xacllo"); // 3-byte literal
//     static_assert(utf16_to_utf8(u"h\ufb00llo") == "h\xef\xac\x80llo"); // 3-byte literal
//     static_assert(utf16_to_utf8(u"h\U0001f574llo") == "h\xf0\x9f\x95\xb4llo"); // 4-byte literal
//     static_assert(utf16_to_utf8(u"h\xdc00llo") == "h\xef\xbf\xbdllo"); // unpaired surrogate
#endif

    tds::tds(const options& opts) {
        impl = make_unique<tds_impl>(opts.server, opts.user, opts.password, opts.app_name, opts.db,
                                     opts.message_handler, opts.count_handler, opts.port,
                                     opts.encrypt, opts.check_certificate, opts.mars, opts.rate_limit,
                                     opts.read_only_intent);

        codepage = opts.codepage;

        if (codepage == 0) {
#ifdef _WIN32
            codepage = GetACP();
#else
            codepage = CP_UTF8; // FIXME - get from LANG
#endif
        }
    }

    tds::~tds() {
        // needs to be defined for unique_ptr<tds_impl> to work
    }

    tds::tds(tds&& that) noexcept {
        impl.swap(that.impl);
        codepage = that.codepage;
    }

    tds_impl::tds_impl(const string& server, string_view user, string_view password,
                       string_view app_name, string_view db, const msg_handler& message_handler,
                       const func_count_handler& count_handler, uint16_t port, encryption_type enc,
                       bool check_certificate, bool mars, unsigned int rate_limit, bool read_only_intent) :
                       message_handler(message_handler), count_handler(count_handler), check_certificate(check_certificate), rate_limit(rate_limit) {
#ifdef _WIN32
        WSADATA wsa_data;

        if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0)
            throw runtime_error("WSAStartup failed.");

        if (server.starts_with("\\\\") || server.starts_with("np:") || server.starts_with("lpc:")) { // named pipe
            auto name = utf8_to_utf16(server);

            if (name.starts_with(u"np:"))
                name = u"\\\\" + name.substr(3) + u"\\pipe\\sql\\query";
            else if (name.starts_with(u"lpc:"))
                name = u"\\\\" + name.substr(4) + u"\\pipe\\SQLLocal\\MSSQLSERVER";

            do {
                pipe.reset(CreateFileW((WCHAR*)name.c_str(), FILE_READ_DATA | FILE_WRITE_DATA, 0, nullptr, OPEN_EXISTING,
                                       FILE_FLAG_OVERLAPPED, nullptr));

                if (pipe.get() != INVALID_HANDLE_VALUE)
                    break;

                if (GetLastError() != ERROR_PIPE_BUSY)
                    throw last_error("CreateFile(" + utf16_to_utf8(name) + ")", GetLastError());

                if (!WaitNamedPipeW((WCHAR*)name.c_str(), NMPWAIT_WAIT_FOREVER))
                    throw last_error("WaitNamedPipe", GetLastError());
            } while (true);
        } else {
#endif
            connect(server, port, user.empty());
            hostname = server;

#ifdef _WIN32
            u_long mode = 1;

            if (ioctlsocket(sock, FIONBIO, &mode) != 0)
                throw formatted_error("ioctlsocket failed ({}).", wsa_error_to_string(WSAGetLastError()));
#else
            if (fcntl(sock, F_SETFL, fcntl(sock, F_GETFL, 0) | O_NONBLOCK) != 0)
                throw formatted_error("fcntl failed to make socket non-blocking (error {})", errno_to_string(errno));
#endif
#ifdef _WIN32
        }
#endif

#if !defined(WITH_OPENSSL) && !defined(_WIN32)
        enc = encryption_type::ENCRYPT_NOT_SUP;
#endif

        t = jthread([this](stop_token stop) { socket_thread_wrap(stop); });

        try {
            send_prelogin_msg(enc, mars);

#if defined(WITH_OPENSSL) || defined(_WIN32)
            if (server_enc != encryption_type::ENCRYPT_NOT_SUP) {
                ssl = make_unique<tds_ssl>(*this);
                mess_event.set();
            }
#endif

            send_login_msg(user, password, server, app_name, db, read_only_intent);

#if defined(WITH_OPENSSL) || defined(_WIN32)
            if (server_enc != encryption_type::ENCRYPT_ON && server_enc != encryption_type::ENCRYPT_REQ)
                ssl.reset();
#endif

            if (this->mars)
                mars_sess = make_unique<smp_session>(*this);
        } catch (...) {
            t.request_stop();
            mess_event.set();
            throw;
        }
    }

    void tds_impl::socket_thread_wrap(stop_token stop) noexcept {
        try {
            socket_thread(stop);
        } catch (...) {
            {
                lock_guard lg(sess.mess_in_lock);
                sess.socket_thread_exc = current_exception();
            }
        }

        sess.mess_in_cv.notify_all();

        if (mars_sess) {
            lock_guard lg(mars_lock);

            for (auto& sess_rw : mars_list) {
                auto& sess = sess_rw.get();

                {
                    lock_guard lg(sess.mess_in_lock);
                    sess.socket_thread_exc = current_exception();
                }

                sess.mess_in_cv.notify_all();
            }
        }
    }

    void tds_impl::socket_thread_read(ringbuf& in_buf) {
        uint8_t buf[4096];

        do {
            size_t to_read = min(in_buf.available(), sizeof(buf));

            if (to_read == 0)
                break;

            auto ret = recv(sock, (char*)buf, (int)to_read, 0);

            if (ret < 0) {
#ifdef _WIN32
                if (WSAGetLastError() == WSAEWOULDBLOCK)
                    break;

                throw formatted_error("recv failed (error {})", wsa_error_to_string(WSAGetLastError()));
#else
                if (errno == EWOULDBLOCK)
                    break;

                throw formatted_error("recv failed (error {})", errno_to_string(errno));
#endif
            }

            if (ret == 0)
                break;

            in_buf.write(span(buf, ret));
        } while (true);
    }

    bool tds_impl::socket_thread_write() {
        while (!mess_out_buf.empty()) {
            auto ret = send(sock, (char*)mess_out_buf.data(), (int)mess_out_buf.size(), 0);

            if (ret < 0) {
#ifdef _WIN32
                if (WSAGetLastError() == WSAEWOULDBLOCK)
                    return false;

                throw formatted_error("send failed (error {})", wsa_error_to_string(WSAGetLastError()));
#else
                if (errno == EWOULDBLOCK)
                    return false;

                throw formatted_error("send failed (error {})", errno_to_string(errno));
#endif
            }

            if (ret == (int)mess_out_buf.size()) {
                mess_out_buf.clear();
                break;
            }

            vector<uint8_t> newbuf{mess_out_buf.begin() + ret, mess_out_buf.end()};
            mess_out_buf.swap(newbuf);
        }

        return true;
    }

    void tds_impl::socket_thread_parse_messages(stop_token stop, ringbuf& in_buf) {
        while (in_buf.size() >= sizeof(tds_header)) {
            tds_header h;

            in_buf.peek(span((uint8_t*)&h, sizeof(tds_header)));

            if ((uint8_t)h.type == 0x53) {
                if (in_buf.size() < sizeof(smp_header))
                    return;

                smp_header smp;

                in_buf.peek(span((uint8_t*)&smp, sizeof(smp_header)));

                if (!mars_sess)
                    throw runtime_error("SMP message received in non-MARS session.");

                if (smp.flags == smp_message_type::FIN) { // ignore FIN acknowledgements
                    if (in_buf.size() >= smp.length)
                        in_buf.discard(smp.length);

                    continue;
                }

                if (smp.length < sizeof(smp_header))
                    throw formatted_error("SMP message length was {}, expected at least {}", smp.length, sizeof(smp_header));

                if (in_buf.size() < smp.length)
                    return;

                vector<uint8_t> buf;

                buf.resize(smp.length);
                in_buf.read(buf);

                {
                    lock_guard lg(mars_lock);

                    for (auto& sess_rw : mars_list) {
                        auto& sess = sess_rw.get();

                        if (sess.sid == smp.sid) {
                            sess.parse_message(stop, buf);
                            break;
                        }
                    }
                }
            } else {
                auto len = htons(h.length);

                if (len < sizeof(tds_header))
                    throw formatted_error("message length was {}, expected at least {}", len, sizeof(tds_header));

                if (in_buf.size() < len)
                    return;

                in_buf.discard(sizeof(tds_header));

                mess m;

                m.type = h.type;

                if (len >= sizeof(tds_header)) {
                    m.payload.resize(len - sizeof(tds_header));
                    in_buf.read(m.payload);
                }

                m.last_packet = h.status & 1;

                spid = htons(h.spid);

                {
                    unique_lock ul(sess.mess_in_lock);

                    if (rate_limit != 0) {
                        sess.rate_limit_cv.wait(ul, stop, [&]() { return sess.mess_list.size() < rate_limit; });

                        if (stop.stop_requested())
                            return;
                    }

                    sess.mess_list.emplace_back(std::move(m));
                }

                sess.mess_in_cv.notify_one();
            }
        }
    }

#if defined(WITH_OPENSSL) || defined(_WIN32)
    void tds_impl::decrypt_messages(ringbuf& in_buf, ringbuf& pt_buf) {
        auto ret = ssl->dec(in_buf);

        if (!ret.empty())
            pt_buf.write(ret);
    }
#endif

#ifdef _WIN32
    void tds_impl::pipe_write() {
        optional<event> write_event;

        while (true) {
            unique_lock lock(mess_out_lock);

            if (mess_out_buf.empty())
                return;

            lock.unlock();

            DWORD tmp, written;
            OVERLAPPED async = {};

            if (!write_event)
                write_event.emplace();
            else
                write_event->reset();

            async.hEvent = write_event->h.get();

            lock.lock();

            auto ret = WriteFile(pipe.get(), mess_out_buf.data(), (DWORD)mess_out_buf.size(),
                                 &tmp, &async);

            if (!ret && GetLastError() != ERROR_IO_PENDING)
                throw last_error("WriteFile", GetLastError());

            if (!GetOverlappedResult(pipe.get(), &async, &written, true))
                throw last_error("GetOverlappedResult", GetLastError());

            if (written > 0) {
                vector<uint8_t> newbuf{mess_out_buf.data() + written, mess_out_buf.data() + mess_out_buf.size()};
                mess_out_buf.swap(newbuf);
            }

            if (mess_out_buf.empty())
                return;
        }
    }
#endif

    void tds_impl::socket_thread(stop_token stop) {
        name_thread("tdscpp thread");

#if defined(WITH_OPENSSL) || defined(_WIN32)
        bool do_ssl = ssl && (server_enc == encryption_type::ENCRYPT_ON || server_enc == encryption_type::ENCRYPT_REQ);
        ringbuf pt_buf(65536);
#endif
        ringbuf in_buf(65536);

#ifdef _WIN32
        if (pipe.get() != INVALID_HANDLE_VALUE) {
            event read_event;
            uint8_t buf[4096];
            DWORD tmp;
            OVERLAPPED async = {};

            async.hEvent = read_event.h.get();

            while (!stop.stop_requested()) {
                read_event.reset();

                auto read_result = ReadFile(pipe.get(), buf, sizeof(buf), &tmp, &async);

                if (!read_result && GetLastError() != ERROR_IO_PENDING)
                    throw last_error("ReadFile", GetLastError());

                while (true) {
                    array<HANDLE, 2> objs;

                    objs[0] = read_event.h.get();
                    objs[1] = mess_event.h.get();

                    auto ret = WaitForMultipleObjects(objs.size(), objs.data(), false, INFINITE);

                    if (ret == WAIT_FAILED)
                        throw formatted_error("WaitForMultipleObjects failed (error {}).", GetLastError());

                    if (ret == WAIT_OBJECT_0) {
                        DWORD read;

                        if (!GetOverlappedResult(pipe.get(), &async, &read, true))
                            throw last_error("GetOverlappedResult", GetLastError());

                        if (read > 0) {
                            in_buf.write(span(buf, read));

                            if (do_ssl) {
                                decrypt_messages(in_buf, pt_buf);
                                socket_thread_parse_messages(stop, pt_buf);
                            } else
                                socket_thread_parse_messages(stop, in_buf);
                        }

                        break;
                    } else if (ret == WAIT_OBJECT_0 + 1) {
                        mess_event.reset();

                        do_ssl = ssl && (server_enc == encryption_type::ENCRYPT_ON || server_enc == encryption_type::ENCRYPT_REQ);

                        if (stop.stop_requested())
                            break;

                        pipe_write();
                    }
                }
            }
        } else {
            event winsock_event;
            bool can_write = false;

            if (WSAEventSelect(sock, winsock_event.h.get(), FD_READ | FD_WRITE | FD_CLOSE) == SOCKET_ERROR)
                throw formatted_error("WSAEventSelect failed (error {}).", wsa_error_to_string(WSAGetLastError()));

            array<HANDLE, 2> objs;

            objs[0] = winsock_event.h.get();
            objs[1] = mess_event.h.get();

            while (!stop.stop_requested()) {
                WSANETWORKEVENTS netev;

                auto ret = WaitForMultipleObjects(objs.size(), objs.data(), false, INFINITE);

                if (ret == WAIT_FAILED)
                    throw formatted_error("WaitForMultipleObjects failed (error {}).", GetLastError());

                if (ret == WAIT_OBJECT_0) {
                    if (WSAEnumNetworkEvents(sock, winsock_event.h.get(), &netev))
                        throw formatted_error("WSAEnumNetworkEvents failed (error {}).", wsa_error_to_string(WSAGetLastError()));

                    if (netev.lNetworkEvents & FD_READ) {
                        socket_thread_read(in_buf);

                        if (do_ssl) {
                            decrypt_messages(in_buf, pt_buf);
                            socket_thread_parse_messages(stop, pt_buf);
                        } else
                            socket_thread_parse_messages(stop, in_buf);
                    }

                    if (netev.lNetworkEvents & FD_WRITE) {
                        lock_guard lg(mess_out_lock);

                        can_write = socket_thread_write();
                    }

                    if (netev.lNetworkEvents & FD_CLOSE) {
                        connected = false;

                        break;
                    }
                } else if (ret == WAIT_OBJECT_0 + 1) {
                    mess_event.reset();

                    do_ssl = ssl && (server_enc == encryption_type::ENCRYPT_ON || server_enc == encryption_type::ENCRYPT_REQ);

                    {
                        lock_guard lg(mess_out_lock);

                        if (can_write && !mess_out_buf.empty())
                            can_write = socket_thread_write();
                    }
                }
            }
        }
#else
        unique_handle epoll{epoll_create1(EPOLL_CLOEXEC)};

        if (epoll.get() == -1)
            throw formatted_error("epoll_create1 failed (error {})", errno_to_string(errno));

        array<struct epoll_event, 2> evs;

        bool poll_out = true;

        evs[0].events = EPOLLIN | EPOLLOUT | EPOLLRDHUP;
        evs[0].data.fd = sock;
        evs[1].events = EPOLLIN;
        evs[1].data.fd = mess_event.h.get();

        for (auto& e : evs) {
            if (epoll_ctl(epoll.get(), EPOLL_CTL_ADD, e.data.fd, &e) == -1)
                throw formatted_error("epoll_ctl failed (error {})", errno_to_string(errno));
        }

        while (!stop.stop_requested()) {
            struct epoll_event ev;

            auto ret = epoll_wait(epoll.get(), &ev, 1, -1);

            if (ret == -1) {
                if (errno == EINTR)
                    continue;

                throw formatted_error("epoll_wait failed (error {})", errno_to_string(errno));
            }

            if (ret != 0) {
                if (ev.data.fd == sock) {
                    if (ev.events & EPOLLIN) {
                        socket_thread_read(in_buf);

#ifdef WITH_OPENSSL
                        if (do_ssl) {
                            decrypt_messages(in_buf, pt_buf);
                            socket_thread_parse_messages(stop, pt_buf);
                        } else
#endif
                            socket_thread_parse_messages(stop, in_buf);
                    }

                    if (ev.events & EPOLLOUT) {
                        lock_guard lg(mess_out_lock);

                        socket_thread_write();

                        if (mess_out_buf.empty()) {
                            auto& e = evs[0];

                            e.events &= ~EPOLLOUT;

                            if (epoll_ctl(epoll.get(), EPOLL_CTL_MOD, e.data.fd, &e) == -1)
                                throw formatted_error("epoll_ctl failed (error {})", errno_to_string(errno));

                            poll_out = false;
                        }
                    }

                    if (ev.events & (EPOLLRDHUP | EPOLLHUP)) {
                        connected = false;

                        break;
                    }
                } else if (ev.data.fd == mess_event.h.get()) {
                    mess_event.reset();

#ifdef WITH_OPENSSL
                    do_ssl = ssl && (server_enc == encryption_type::ENCRYPT_ON || server_enc == encryption_type::ENCRYPT_REQ);
#endif

                    {
                        lock_guard lg(mess_out_lock);

                        if (!poll_out && !mess_out_buf.empty()) {
                            auto& e = evs[0];

                            e.events |= EPOLLOUT;

                            if (epoll_ctl(epoll.get(), EPOLL_CTL_MOD, e.data.fd, &e) == -1)
                                throw formatted_error("epoll_ctl failed (error {})", errno_to_string(errno));

                            poll_out = true;
                        }
                    }
                }
            }
        }
#endif
    }

    smp_session::smp_session(tds_impl& tds) : tds(tds) {
        smp_header h;

        // FIXME - link this to rate_limit option
        recv_wndw = 4;

        h.smid = 0x53;
        h.flags = smp_message_type::SYN;
        h.length = sizeof(smp_header);
        h.seqnum = 0;
        h.wndw = recv_wndw;

        auto sp = span((const uint8_t*)&h, sizeof(smp_header));

        {
            lock_guard lg(tds.mars_lock);

            h.sid = sid = tds.last_sid;
            tds.last_sid++;

            tds.sess.send_raw(sp);

            tds.mars_list.emplace_back(*this);
        }
    }

    smp_session::~smp_session() {
        {
            lock_guard lg(tds.mars_lock);

            for (auto it = tds.mars_list.begin(); it != tds.mars_list.end(); it++) {
                auto& sess = it->get();

                if (&sess == this) {
                    tds.mars_list.erase(it);
                    break;
                }
            }
        }

        try {
            smp_header h;

            h.smid = 0x53;
            h.flags = smp_message_type::FIN;
            h.sid = sid;
            h.length = sizeof(smp_header);
            h.seqnum = seqnum - 1;
            h.wndw = recv_wndw;

            auto sp = span((const uint8_t*)&h, sizeof(smp_header));

            tds.sess.send_raw(sp);
        } catch (...) {
        }
    }

    void smp_session::send_msg(enum tds_msg type, span<const uint8_t> msg) {
        vector<uint8_t> buf;

        do {
            size_t to_send = min(msg.size(), tds.packet_size - sizeof(tds_header));

            buf.reserve(sizeof(smp_header) + sizeof(tds_header) + to_send);
            buf.resize(sizeof(smp_header) + sizeof(tds_header));

            auto& h1 = *(smp_header*)buf.data();

            h1.smid = 0x53;
            h1.flags = smp_message_type::DATA;
            h1.sid = sid;
            h1.length = (uint32_t)(sizeof(smp_header) + sizeof(tds_header) + to_send);
            h1.seqnum = seqnum;
            h1.wndw = recv_wndw;

            seqnum++;

            auto& h2 = *(tds_header*)(buf.data() + sizeof(smp_header));

            h2.type = type;
            h2.status = to_send == msg.size() ? 1 : 0; // 1 == last message
            h2.length = htons((uint16_t)(to_send + sizeof(tds_header)));
            h2.spid = 0;
            h2.packet_id = 0; // FIXME? "Currently ignored" according to spec
            h2.window = 0;

            buf.insert(buf.end(), msg.data(), msg.data() + to_send);

            tds.sess.send_raw(buf);

            msg = msg.subspan(to_send);
        } while (!msg.empty());
    }

    template<typename T>
    void wait_for_msg2(T& sess, enum tds_msg& type, vector<uint8_t>& payload, bool* last_packet) {
        mess m;
        bool disconnected = false;

        {
            unique_lock ul(sess.mess_in_lock);

            sess.mess_in_cv.wait(ul, [&]() { return !sess.mess_list.empty() || sess.socket_thread_exc || !sess.tds.connected; });

            if (!sess.mess_list.empty()) {
                auto& m2 = sess.mess_list.front();

                m.type = m2.type;
                m.payload.swap(m2.payload);
                m.last_packet = m2.last_packet;

                sess.mess_list.pop_front();
            } else
                disconnected = true;
        }

        if (disconnected) {
            if (sess.socket_thread_exc)
                rethrow_exception(sess.socket_thread_exc);

            throw runtime_error("Disconnected.");
        }

        if (sess.tds.rate_limit != 0)
            sess.rate_limit_cv.notify_one();

        type = m.type;
        payload.swap(m.payload);

        if (last_packet)
            *last_packet = m.last_packet;
    }

    void smp_session::wait_for_msg(enum tds_msg& type, vector<uint8_t>& payload, bool* last_packet) {
        wait_for_msg2(*this, type, payload, last_packet);
    }

    void smp_session::send_ack() {
        smp_header h;

        h.smid = 0x53;
        h.flags = smp_message_type::ACK;
        h.sid = sid;
        h.length = sizeof(smp_header);
        h.seqnum = seqnum - 1;
        h.wndw = recv_wndw;

        auto sp = span((const uint8_t*)&h, sizeof(smp_header));

        tds.sess.send_raw(sp);
    }

    void smp_session::parse_message(stop_token stop, span<const uint8_t> msg) {
        auto& s = *(smp_header*)msg.data();

        // FIXME - honour server-side rate limiting

        if (s.seqnum == recv_wndw) {
            recv_wndw += 4;

            send_ack();
        }

        switch (s.flags) {
            case smp_message_type::ACK:
                throw runtime_error("FIXME - handle SMP ACK message");

            case smp_message_type::DATA: {
                if (msg.size() < sizeof(smp_header) + sizeof(tds_header))
                    throw formatted_error("SMP DATA message was {} bytes, expected at least {}.", msg.size(), sizeof(smp_header) + sizeof(tds_header));

                auto& h = *(tds_header*)(msg.data() + sizeof(smp_header));

                auto len = htons(h.length);

                if (len < sizeof(tds_header))
                    throw formatted_error("message length was {}, expected at least {}", len, sizeof(tds_header));

                mess m;

                m.type = h.type;

                if (len >= sizeof(tds_header)) {
                    m.payload.assign(msg.data() + sizeof(smp_header) + sizeof(tds_header),
                                     msg.data() + sizeof(smp_header) + len);
                }

                m.last_packet = h.status & 1;

                // FIXME - do SMP sessions have separate SPIDs?

                {
                    unique_lock ul(mess_in_lock);

                    if (tds.rate_limit != 0) {
                        rate_limit_cv.wait(ul, stop, [&]() { return mess_list.size() < tds.rate_limit; });

                        if (stop.stop_requested())
                            return;
                    }

                    mess_list.emplace_back(std::move(m));
                }

                mess_in_cv.notify_one();

                break;
            }

            default:
                throw formatted_error("Server sent unexpected SMP message type {:02x}.", (uint8_t)s.flags);
        }
    }

    tds_impl::~tds_impl() {
        if (t.joinable()) {
            t.request_stop();

            try {
                mess_event.set();
            } catch (...) {
            }
        }

#ifdef _WIN32
        if (sock != INVALID_SOCKET)
            closesocket(sock);
#else
        if (sock != 0)
            close(sock);
#endif
    }

    void tds_impl::connect(const string& server, uint16_t port, bool get_fqdn) {
        struct addrinfo hints;
        struct addrinfo* res;
        struct addrinfo* orig_res;
        int ret;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        ret = getaddrinfo(server.c_str(), nullptr, &hints, &res);

        if (ret != 0)
            throw formatted_error("getaddrinfo returned {}", ret);

        orig_res = res;
#ifdef _WIN32
        sock = INVALID_SOCKET;
#else
        sock = 0;
#endif

        do {
            char hostname[NI_MAXHOST];

            sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

#ifdef _WIN32
            if (sock == INVALID_SOCKET)
                continue;
#else
            if (sock < 0)
                continue;
#endif

            if (res->ai_family == AF_INET)
                ((struct sockaddr_in*)res->ai_addr)->sin_port = htons(port);
            else if (res->ai_family == AF_INET6)
                ((struct sockaddr_in6*)res->ai_addr)->sin6_port = htons(port);
            else {
#ifdef _WIN32
                closesocket(sock);
                sock = INVALID_SOCKET;
#else
                close(sock);
                sock = 0;
#endif
                continue;
            }

            if (::connect(sock, res->ai_addr, (int)res->ai_addrlen) != 0) {
#ifdef _WIN32
                closesocket(sock);
                sock = INVALID_SOCKET;
#else
                close(sock);
                sock = 0;
#endif
                continue;
            }

            if (get_fqdn) {
                if (getnameinfo(res->ai_addr, (socklen_t)res->ai_addrlen, hostname, sizeof(hostname), nullptr, 0, 0) == 0)
                    fqdn = hostname;
            }

            break;
        } while ((res = res->ai_next) != nullptr);

        freeaddrinfo(orig_res);

#ifdef _WIN32
        if (sock == INVALID_SOCKET)
            throw formatted_error("Could not connect to {}:{}.", server, port);
#else
        if (sock <= 0)
            throw formatted_error("Could not connect to {}:{}.", server, port);
#endif
    }

    void tds_impl::send_prelogin_msg(enum encryption_type encrypt, bool mars) {
        vector<uint8_t> msg;
        vector<login_opt> opts;
        login_opt_version lov;
        size_t size, off;

        // FIXME - allow the user to specify this
        static const string_view instance = "MSSQLServer";

        // version

        lov.major = 9;
        lov.minor = 0;
        lov.build = 0;
        lov.subbuild = 0;

        opts.emplace_back(tds_login_opt_type::version, span{(uint8_t*)&lov, sizeof(lov)});

        // encryption

        opts.emplace_back(tds_login_opt_type::encryption, encrypt);

        // instopt

        opts.emplace_back(tds_login_opt_type::instopt, span((uint8_t*)instance.data(), instance.size()));

        // MARS

        opts.emplace_back(tds_login_opt_type::mars, (uint8_t)(mars ? 1 : 0));

        size = (sizeof(tds_login_opt) * opts.size()) + sizeof(enum tds_login_opt_type);
        off = size;

        for (const auto& opt : opts) {
            size += opt.payload.size();

            if (opt.type == tds_login_opt_type::instopt)
                size++;
        }

        msg.resize(size);

        auto tlo = (tds_login_opt*)msg.data();

        for (const auto& opt : opts) {
            tlo->type = opt.type;
            tlo->offset = htons((uint16_t)off);

            if (opt.type == tds_login_opt_type::instopt)
                tlo->length = htons((uint16_t)opt.payload.size() + 1);
            else
                tlo->length = htons((uint16_t)opt.payload.size());

            memcpy(msg.data() + off, opt.payload.data(), opt.payload.size());
            off += opt.payload.size();

            // instopt is null-terminated
            if (opt.type == tds_login_opt_type::instopt) {
                msg[off] = 0;
                off++;
            }

            tlo++;
        }

        tlo->type = tds_login_opt_type::terminator;

        sess.send_msg(tds_msg::prelogin, msg);

        enum tds_msg type;
        vector<uint8_t> payload;

        sess.wait_for_msg(type, payload);
        // FIXME - timeout

        if (type != tds_msg::tabular_result)
            throw formatted_error("Received message type {}, expected tabular_result", (int)type);

        span sp = payload;

        while (sp.size() > sizeof(tds_login_opt)) {
            tlo = (tds_login_opt*)sp.data();

            if (tlo->type == tds_login_opt_type::terminator)
                break;

            auto off = htons(tlo->offset);
            auto len = htons(tlo->length);

            if (payload.size() < off + len)
                throw runtime_error("Malformed PRELOGIN response.");

            auto pl = span(payload.data() + off, len);

            switch (tlo->type) {
                case tds_login_opt_type::encryption:
                    if (pl.size() < sizeof(enum encryption_type))
                        throw formatted_error("Returned encryption type was {} bytes, expected {}.", pl.size(), sizeof(enum encryption_type));

                    server_enc = *(enum encryption_type*)pl.data();
                break;

                case tds_login_opt_type::mars:
                    if (pl.empty())
                        throw formatted_error("Returned MARS value was empty, expected 1 byte.");

                    this->mars = pl[0] != 0;
                break;

                default:
                break;
            }

            sp = sp.subspan(sizeof(tds_login_opt));
        }

#if !defined(WITH_OPENSSL) && !defined(_WIN32)
        if (server_enc == encryption_type::ENCRYPT_REQ)
            throw runtime_error("Server required encryption, but tdscpp has been compiled without TLS support.");
#endif
    }

    void tds_impl::send_login_msg(string_view user, string_view password, string_view server,
                                  string_view app_name, string_view db, bool read_only_intent) {
        enum tds_msg type;
        vector<uint8_t> payload, sspi;
#ifdef _WIN32
        u16string spn;
        unique_ptr<sspi_handle> sspih;
#elif defined(HAVE_GSSAPI)
        string spn;
        gss_cred_id_t cred_handle = 0;
        gss_ctx_id_t ctx_handle = GSS_C_NO_CONTEXT;
#endif

        auto user_u16 = utf8_to_utf16(user);
        auto password_u16 = utf8_to_utf16(password);

#ifdef _WIN32
        if (user.empty() && pipe.get() == INVALID_HANDLE_VALUE) {
#else
        if (user.empty()) {
#endif
            if (fqdn.empty())
                throw runtime_error("Could not do SSPI authentication as could not find server FQDN.");

#ifdef _WIN32
            spn = u"MSSQLSvc/" + utf8_to_utf16(fqdn);

            SECURITY_STATUS sec_status;
            TimeStamp timestamp;
            SecBuffer outbuf;
            SecBufferDesc out;
            uint32_t context_attr;

            sspih = make_unique<sspi_handle>();

            outbuf.cbBuffer = 0;
            outbuf.BufferType = SECBUFFER_TOKEN;
            outbuf.pvBuffer = nullptr;

            out.ulVersion = SECBUFFER_VERSION;
            out.cBuffers = 1;
            out.pBuffers = &outbuf;

            sec_status = sspih->init_security_context(spn.c_str(), ISC_REQ_ALLOCATE_MEMORY, SECURITY_NATIVE_DREP,
                                                      nullptr, &out, &context_attr, &timestamp);

            sspi.assign((uint8_t*)outbuf.pvBuffer, (uint8_t*)outbuf.pvBuffer + outbuf.cbBuffer);

            if (outbuf.pvBuffer)
                FreeContextBuffer(outbuf.pvBuffer);

            if (sec_status != SEC_E_OK && sec_status != SEC_I_CONTINUE_NEEDED && sec_status != SEC_I_COMPLETE_AND_CONTINUE)
                throw formatted_error("InitializeSecurityContext returned unexpected status {}", (enum sec_error)sec_status);
#elif defined(HAVE_GSSAPI)
            spn = "MSSQLSvc/" + fqdn;

            OM_uint32 major_status, minor_status;
            gss_buffer_desc recv_tok, send_tok, name_buf;
            unique_ptr<gss_name_t, gss_name_deleter> gss_name;

            if (cred_handle != 0) {
                major_status = gss_acquire_cred(&minor_status, GSS_C_NO_NAME, GSS_C_INDEFINITE, GSS_C_NO_OID_SET,
                                                GSS_C_INITIATE, &cred_handle, nullptr, nullptr);

                if (major_status != GSS_S_COMPLETE)
                    throw gss_error("gss_acquire_cred", major_status, minor_status);
            }

            name_buf.length = spn.length();
            name_buf.value = (void*)spn.data();

            {
                gss_name_t tmp = nullptr;

                major_status = gss_import_name(&minor_status, &name_buf, GSS_C_NO_OID, &tmp);

                gss_name.reset(tmp);

                if (major_status != GSS_S_COMPLETE) {
                    gss_release_cred(&minor_status, &cred_handle);
                    throw gss_error("gss_import_name", major_status, minor_status);
                }
            }

            recv_tok.length = 0;
            recv_tok.value = nullptr;

            major_status = gss_init_sec_context(&minor_status, cred_handle, &ctx_handle, gss_name.get(),
                                                GSS_C_NO_OID, GSS_C_DELEG_FLAG, GSS_C_INDEFINITE,
                                                GSS_C_NO_CHANNEL_BINDINGS, &recv_tok, nullptr, &send_tok,
                                                nullptr, nullptr);

            if (major_status != GSS_S_CONTINUE_NEEDED && major_status != GSS_S_COMPLETE) {
                gss_release_cred(&minor_status, &cred_handle);
                throw gss_error("gss_init_sec_context", major_status, minor_status);
            }

            if (send_tok.length != 0) {
                sspi.assign((uint8_t*)send_tok.value, (uint8_t*)send_tok.value + send_tok.length);

                gss_release_buffer(&minor_status, &send_tok);
            }

            gss_delete_sec_context(&minor_status, &ctx_handle, GSS_C_NO_BUFFER);
            gss_release_cred(&minor_status, &cred_handle);
#else
            throw runtime_error("No username given and Kerberos support not compiled in.");
#endif
        }

        u16string client_name;

        {
            char s[255];

            if (gethostname(s, sizeof(s)) != 0) {
#ifdef _WIN32
                throw formatted_error("gethostname failed (error {})", WSAGetLastError());
#else
                throw formatted_error("gethostname failed (error {})", errno);
#endif
            }

            client_name = utf8_to_utf16(s);
        }

        // FIXME - client PID
        // FIXME - option flags (1, 2, 3)
        // FIXME - collation
        // FIXME - locale name?

        send_login_msg2(0x74000004, packet_size, 0xf8f28306, 0x5ab7, 0, 0xe0, 0x03, read_only_intent, 0x08, 0x436,
                        client_name, user_u16, password_u16, utf8_to_utf16(app_name), utf8_to_utf16(server), u"", u"us_english",
                        utf8_to_utf16(db), sspi, u"", u"", sess);

        // FIXME - timeout

        bool received_loginack;
#ifdef _WIN32
        bool go_again;
#endif

        do {
#ifdef _WIN32
            go_again = false;
#endif
            bool last_packet;
            vector<uint8_t> buf;
            list<vector<uint8_t>> tokens;
            vector<column> buf_columns;
#ifdef _WIN32
            vector<uint8_t> sspibuf;
#endif

            do {
                sess.wait_for_msg(type, payload, &last_packet);
                // FIXME - timeout

                if (type != tds_msg::tabular_result)
                    throw formatted_error("Received message type {}, expected tabular_result", (int)type);

                buf.insert(buf.end(), payload.begin(), payload.end());

                {
                    auto sp = parse_tokens(buf, tokens, buf_columns);

                    if (sp.size() != buf.size()) {
                        vector<uint8_t> newbuf{sp.begin(), sp.end()};
                        buf.swap(newbuf);
                    }
                }

                if (last_packet && !buf.empty())
                    throw formatted_error("Data remaining in buffer");

                received_loginack = false;

                vector<uint8_t> t;

                while (!tokens.empty()) {
                    t.swap(tokens.front());
                    tokens.pop_front();

                    auto type = (token)t[0];

                    auto sp = span<const uint8_t>(t).subspan(1);

                    switch (type) {
                        case token::DONE:
                        case token::DONEINPROC:
                        case token::DONEPROC:
                            if (sp.size() < sizeof(tds_done_msg))
                                throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), sizeof(tds_done_msg));

                            break;

                        case token::LOGINACK:
                        case token::INFO:
                        case token::TDS_ERROR:
                        case token::ENVCHANGE:
                        {
                            if (sp.size() < sizeof(uint16_t))
                                throw formatted_error("Short {} message ({} bytes, expected at least 2).", type, sp.size());

                            auto len = *(uint16_t*)&sp[0];

                            sp = sp.subspan(sizeof(uint16_t));

                            if (sp.size() < len)
                                throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), len);

                            if (type == token::LOGINACK) {
                                handle_loginack_msg(sp.subspan(0, len));
                                received_loginack = true;
                            } else if (type == token::INFO) {
                                if (message_handler)
                                    handle_info_msg(sp.subspan(0, len), false);
                            } else if (type == token::TDS_ERROR) {
                                if (message_handler)
                                    handle_info_msg(sp.subspan(0, len), true);

                                throw formatted_error("Login failed: {}", utf16_to_utf8(extract_message(sp.subspan(0, len))));
                            } else if (type == token::ENVCHANGE)
                                handle_envchange_msg(sp.subspan(0, len));

                            break;
                        }

#ifdef _WIN32
                        case token::SSPI: // FIXME - handle doing this with GSSAPI
                        {
                            if (sp.size() < sizeof(uint16_t))
                                throw formatted_error("Short {} message ({} bytes, expected at least 2).", type, sp.size());

                            auto len = *(uint16_t*)&sp[0];

                            sp = sp.subspan(sizeof(uint16_t));

                            if (sp.size() < len)
                                throw formatted_error("Short SSPI token ({} bytes, expected {}).", type, sp.size(), len);

                            if (!sspih)
                                throw runtime_error("SSPI token received, but no current SSPI context.");

                            sspibuf.assign(sp.data(), sp.data() + len);
                            go_again = true;

                            break;
                        }
#endif

                        case token::FEATUREEXTACK:
                        {
                            while (true) {
                                auto feature = (enum tds_feature)sp[0];

                                if (feature == tds_feature::TERMINATOR)
                                    break;

                                auto len = *(uint32_t*)&sp[1];

                                if (feature == tds_feature::UTF8_SUPPORT && len >= 1)
                                    has_utf8 = (uint8_t)sp[1 + sizeof(uint32_t)];

                                sp = sp.subspan(1 + sizeof(uint32_t) + len);
                            }

                            break;
                        }

                        default:
                            break;
                    }
                }
            } while (!last_packet);

#ifdef _WIN32
            if (go_again)
                send_sspi_msg(&sspih->cred_handle, &sspih->ctx_handle, spn, sspibuf, server_enc, sess);
#endif
            if (received_loginack)
                break;
        } while (true);
    }

#if defined(WITH_OPENSSL) || defined(_WIN32)
    void main_session::send_raw(span<const uint8_t> buf, bool do_ssl)
#else
    void main_session::send_raw(span<const uint8_t> buf)
#endif
    {
        lock_guard lg(tds.mess_out_lock);

#if defined(WITH_OPENSSL) || defined(_WIN32)
        if (do_ssl && tds.ssl) {
            auto enc = tds.ssl->enc(buf);

            tds.mess_out_buf.insert(tds.mess_out_buf.end(), enc.begin(), enc.end());
        } else {
#endif
            tds.mess_out_buf.insert(tds.mess_out_buf.end(), buf.begin(), buf.end());

#if defined(WITH_OPENSSL) || defined(_WIN32)
        }
#endif

        tds.mess_event.set();
    }

#if defined(WITH_OPENSSL) || defined(_WIN32)
    void main_session::send_msg(enum tds_msg type, span<const uint8_t> msg, bool do_ssl)
#else
    void main_session::send_msg(enum tds_msg type, span<const uint8_t> msg)
#endif
    {
        do {
            size_t to_send = min(msg.size(), tds.packet_size - sizeof(tds_header));

            vector<uint8_t> buf;

            buf.resize(to_send + sizeof(tds_header));

            auto& h = *(tds_header*)buf.data();

            h.type = type;
            h.status = to_send == msg.size() ? 1 : 0; // 1 == last message
            h.length = htons((uint16_t)(to_send + sizeof(tds_header)));
            h.spid = 0;
            h.packet_id = 0; // FIXME? "Currently ignored" according to spec
            h.window = 0;

            memcpy(buf.data() + sizeof(tds_header), msg.data(), to_send);

#if defined(WITH_OPENSSL) || defined(_WIN32)
            send_raw(buf, do_ssl);
#else
            send_raw(buf);
#endif

            msg = msg.subspan(to_send);
        } while (!msg.empty());
    }

    void main_session::wait_for_msg(enum tds_msg& type, vector<uint8_t>& payload, bool* last_packet) {
        wait_for_msg2(*this, type, payload, last_packet);
    }

    void tds_impl::handle_info_msg(span<const uint8_t> sp, bool error) const {
        if (sp.size() < sizeof(tds_info_msg))
            throw formatted_error("Short INFO message ({} bytes, expected at least 6).", sp.size());

        auto tim = (tds_info_msg*)sp.data();

        sp = sp.subspan(sizeof(tds_info_msg));

        if (sp.size() < sizeof(uint16_t))
            throw formatted_error("Short INFO message ({} bytes left, expected at least 2).", sp.size());

        auto msg_len = *(uint16_t*)sp.data();
        sp = sp.subspan(sizeof(uint16_t));

        if (sp.size() < msg_len * sizeof(char16_t)) {
            throw formatted_error("Short INFO message ({} bytes left, expected at least {}).",
                                  sp.size(), msg_len * sizeof(char16_t));
        }

        auto msg = u16string_view((char16_t*)sp.data(), msg_len);
        sp = sp.subspan(msg_len * sizeof(char16_t));

        if (sp.size() < sizeof(uint8_t))
            throw formatted_error("Short INFO message ({} bytes left, expected at least 1).", sp.size());

        auto server_name_len = (uint8_t)sp[0];
        sp = sp.subspan(sizeof(uint8_t));

        if (sp.size() < server_name_len * sizeof(char16_t)) {
            throw formatted_error("Short INFO message ({} bytes left, expected at least {}).",
                                  sp.size(), server_name_len * sizeof(char16_t));
        }

        auto server_name = u16string_view((char16_t*)sp.data(), server_name_len);
        sp = sp.subspan(server_name_len * sizeof(char16_t));

        if (sp.size() < sizeof(uint8_t))
            throw formatted_error("Short INFO message ({} bytes left, expected at least 1).", sp.size());

        auto proc_name_len = (uint8_t)sp[0];
        sp = sp.subspan(sizeof(uint8_t));

        if (sp.size() < proc_name_len * sizeof(char16_t)) {
            throw formatted_error("Short INFO message ({} bytes left, expected at least {}).",
                                  sp.size(), proc_name_len * sizeof(char16_t));
        }

        auto proc_name = u16string_view((char16_t*)sp.data(), proc_name_len);
        sp = sp.subspan(proc_name_len * sizeof(char16_t));

        if (sp.size() < sizeof(int32_t))
            throw formatted_error("Short INFO message ({} bytes left, expected at least 4).", sp.size());

        auto line_number = *(int32_t*)sp.data();

        message_handler(utf16_to_utf8(server_name), utf16_to_utf8(msg), utf16_to_utf8(proc_name), tim->msgno, line_number,
                        tim->state, tim->severity, error);
    }

    static u16string to_u16string(uint64_t num) {
        char16_t s[22], *p;

        if (num == 0)
            return u"0";

        s[21] = 0;
        p = &s[21];

        while (num != 0) {
            p = &p[-1];

            *p = (char16_t)((num % 10) + '0');

            num /= 10;
        }

        return p;
    }

    // FIXME - can we do static assert if no. of question marks different from no. of parameters?
    void query::do_query(tds& conn, u16string_view q) {
        if (!params.empty()) {
            u16string q2;
            bool in_quotes = false;
            unsigned int param_num = 1;

            // replace ? in q with parameters

            q2.reserve(q.length());

            for (unsigned int i = 0; i < q.length(); i++) {
                if (q[i] == '\'')
                    in_quotes = !in_quotes;

                if (q[i] == '?' && !in_quotes) {
                    q2 += u"@P" + to_u16string(param_num);
                    param_num++;
                } else
                    q2 += q[i];
            }

            rpc r1(conn, u"sp_prepare", handle, create_params_string(), q2, 1); // 1 means return metadata

            while (r1.fetch_row()) { }

            cols = r1.cols;
        } else {
            rpc r1(conn, u"sp_prepare", handle, u"", q, 1); // 1 means return metadata

            while (r1.fetch_row()) { }

            cols = r1.cols;
        }

        if (handle.is_null)
            throw runtime_error("sp_prepare failed.");

        r2 = make_unique<rpc>(conn, u"sp_execute", static_cast<value>(handle), params);
    }

    void query::do_query(session& sess, u16string_view q) {
        this->sess = sess;

        if (!params.empty()) {
            u16string q2;
            bool in_quotes = false;
            unsigned int param_num = 1;

            // replace ? in q with parameters

            q2.reserve(q.length());

            for (unsigned int i = 0; i < q.length(); i++) {
                if (q[i] == '\'')
                    in_quotes = !in_quotes;

                if (q[i] == '?' && !in_quotes) {
                    q2 += u"@P" + to_u16string(param_num);
                    param_num++;
                } else
                    q2 += q[i];
            }

            rpc r1(sess, u"sp_prepare", handle, create_params_string(), q2, 1); // 1 means return metadata

            while (r1.fetch_row()) { }

            cols = r1.cols;
        } else {
            rpc r1(sess, u"sp_prepare", handle, u"", q, 1); // 1 means return metadata

            while (r1.fetch_row()) { }

            cols = r1.cols;
        }

        if (handle.is_null)
            throw runtime_error("sp_prepare failed.");

        r2 = make_unique<rpc>(sess, u"sp_execute", static_cast<value>(handle), params);
    }

    uint16_t query::num_columns() const {
        return (uint16_t)cols.size();
    }

    bool query::fetch_row() {
        if (!r2->fetch_row())
            return false;

        // cols is only set if sp_prepare can determine what the column types will be
        if (cols.empty())
            cols = r2->cols;
        else {
            for (size_t i = 0; i < cols.size(); i++) {
                cols[i].val.swap(r2->cols[i].val);
                cols[i].is_null = r2->cols[i].is_null;
            }
        }

        return true;
    }

    bool query::fetch_row_no_wait() {
        if (!r2->fetch_row_no_wait())
            return false;

        // cols is only set if sp_prepare can determine what the column types will be
        if (cols.empty())
            cols = r2->cols;
        else {
            for (size_t i = 0; i < cols.size(); i++) {
                cols[i].val.swap(r2->cols[i].val);
                cols[i].is_null = r2->cols[i].is_null;
            }
        }

        return true;
    }

    const column& query::operator[](uint16_t i) const {
        return cols[i];
    }

    column& query::operator[](uint16_t i) {
        return cols[i];
    }

    query::~query() {
        try {
            r2.reset(nullptr);

            // FIXME

            if (sess) {
                rpc r(sess->get(), u"sp_unprepare", static_cast<value>(handle));

                while (r.fetch_row()) { }
            } else {
                rpc r(conn, u"sp_unprepare", static_cast<value>(handle));

                while (r.fetch_row()) { }
            }
        } catch (...) {
            // can't throw inside destructor
        }
    }

    u16string type_to_string(enum sql_type type, size_t length, uint8_t precision, uint8_t scale, u16string_view collation, u16string_view clr_name) {
        switch (type) {
            case sql_type::TINYINT:
                return u"TINYINT";

            case sql_type::SMALLINT:
                return u"SMALLINT";

            case sql_type::INT:
                return u"INT";

            case sql_type::BIGINT:
                return u"BIGINT";

            case sql_type::INTN:
                switch (length) {
                    case sizeof(uint8_t):
                        return u"TINYINT";

                    case sizeof(int16_t):
                        return u"SMALLINT";

                    case sizeof(int32_t):
                        return u"INT";

                    case sizeof(int64_t):
                        return u"BIGINT";

                    default:
                        throw formatted_error("INTN has invalid length {}.", length);
                }

            case sql_type::NVARCHAR:
                if (length > 8000)
                    return u"NVARCHAR(MAX)";
                else
                    return u"NVARCHAR(" + to_u16string(length == 0 ? 1 : (length / sizeof(char16_t))) + u")";

            case sql_type::NCHAR:
                return u"NCHAR(" + to_u16string(length == 0 ? 1 : (length / sizeof(char16_t))) + u")";

            case sql_type::VARCHAR:
                if (collation.empty()) {
                    if (length > 8000)
                        return u"VARCHAR(MAX)";
                    else
                        return u"VARCHAR(" + to_u16string(length == 0 ? 1 : length) + u")";
                } else {
                    if (length > 8000)
                        return u"VARCHAR(MAX) COLLATE " + u16string(collation);
                    else
                        return u"VARCHAR(" + to_u16string(length == 0 ? 1 : length) + u") COLLATE " + u16string(collation);
                }

            case sql_type::CHAR:
                return u"CHAR(" + to_u16string(length == 0 ? 1 : length) + u")";

            case sql_type::FLTN:
                switch (length) {
                    case 4:
                        return u"REAL";

                    case 8:
                        return u"FLOAT";

                    default:
                        throw formatted_error("FLTN has invalid length {}.", length);
                }

            case sql_type::DATE:
                return u"DATE";

            case sql_type::TIME:
                return u"TIME(" + to_u16string(scale) + u")";

            case sql_type::DATETIME:
                return u"DATETIME";

            case sql_type::DATETIME2:
                return u"DATETIME2(" + to_u16string(scale) + u")";

            case sql_type::DATETIMEOFFSET:
                return u"DATETIMEOFFSET(" + to_u16string(scale) + u")";

            case sql_type::VARBINARY:
                if (length > 8000)
                    return u"VARBINARY(MAX)";
                else
                    return u"VARBINARY(" + to_u16string(length == 0 ? 1 : length) + u")";

            case sql_type::BINARY:
                return u"BINARY(" + to_u16string(length == 0 ? 1 : length) + u")";

            case sql_type::BITN:
                return u"BIT";

            case sql_type::DATETIM4:
                return u"SMALLDATETIME";

            case sql_type::DATETIMN:
                switch (length) {
                    case 4:
                        return u"SMALLDATETIME";

                    case 8:
                        return u"DATETIME";

                    default:
                        throw formatted_error("DATETIMN has invalid length {}.", length);
                }

            case sql_type::FLOAT:
                return u"FLOAT";

            case sql_type::REAL:
                return u"REAL";

            case sql_type::BIT:
                return u"BIT";

            case sql_type::DECIMAL:
            case sql_type::NUMERIC:
                return u"NUMERIC(" + to_u16string(precision) + u"," + to_u16string(scale) + u")";

            case sql_type::TEXT:
                return u"TEXT";

            case sql_type::NTEXT:
                return u"NTEXT";

            case sql_type::IMAGE:
                return u"IMAGE";

            case sql_type::MONEYN:
                switch (length) {
                    case 4:
                        return u"SMALLMONEY";

                    case 8:
                        return u"MONEY";

                    default:
                        throw formatted_error("MONEYN has invalid length {}.", length);
                }

            case sql_type::MONEY:
                return u"MONEY";

            case sql_type::SMALLMONEY:
                return u"SMALLMONEY";

            case sql_type::UNIQUEIDENTIFIER:
                return u"UNIQUEIDENTIFIER";

            case sql_type::XML:
                return u"XML";

            case sql_type::UDT:
                if (clr_name == u"Microsoft.SqlServer.Types.SqlHierarchyId, Microsoft.SqlServer.Types, Version=11.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91")
                    return u"HIERARCHYID";

                throw formatted_error("Could not get type string for UDT type {}.", utf16_to_utf8(clr_name));

            default:
                throw formatted_error("Could not get type string for {}.", type);
        }
    }

    u16string query::create_params_string() {
        unsigned int num = 1;
        u16string s;

        for (const auto& p : params) {
            if (!s.empty())
                s += u", ";

            s += u"@P" + to_u16string(num) + u" ";

            if (p.type == sql_type::VARCHAR && p.coll.utf8 && !conn.impl->coll.utf8) {
                auto s_len = utf8_to_utf16_len(string_view{(char*)p.val.data(), p.val.size()});

                s += type_to_string(sql_type::NVARCHAR, s_len * sizeof(char16_t), 0, 0, u"", u"");
            } else
                s += type_to_string(p.type, p.val.size(), p.precision, p.scale, u"", p.clr_name);

            num++;
        }

        return s;
    }

    map<u16string, col_info> get_col_info(tds_or_session auto& n, u16string_view table, u16string_view db) {
        map<u16string, col_info> info;

        {
            query sq(n, no_check(uR"(SELECT columns.name,
    columns.system_type_id,
    columns.max_length,
    columns.precision,
    columns.scale,
    columns.collation_name,
    columns.is_nullable,
    COLLATIONPROPERTY(columns.collation_name, 'CodePage'),
    assembly_types.assembly_qualified_name
FROM )" + (db.empty() ? u"" : (u16string(db) + u".")) + uR"(sys.columns
LEFT JOIN )" + (db.empty() ? u"" : (u16string(db) + u".")) + uR"(sys.assembly_types ON assembly_types.user_type_id = columns.user_type_id
WHERE columns.object_id = OBJECT_ID(?))"), db.empty() ? table : (u16string(db) + u"." + u16string(table)));

            while (sq.fetch_row()) {
                auto type = (sql_type)(unsigned int)sq[1];
                auto nullable = (unsigned int)sq[6] != 0;

                if (nullable) {
                    switch (type) {
                        case sql_type::TINYINT:
                        case sql_type::SMALLINT:
                        case sql_type::INT:
                        case sql_type::BIGINT:
                            type = sql_type::INTN;
                            break;

                        case sql_type::REAL:
                        case sql_type::FLOAT:
                            type = sql_type::FLTN;
                            break;

                        case sql_type::DATETIME:
                        case sql_type::DATETIM4:
                            type = sql_type::DATETIMN;
                            break;

                        case sql_type::MONEY:
                        case sql_type::SMALLMONEY:
                            type = sql_type::MONEYN;
                            break;

                        default:
                            break;
                    }
                }

                info.emplace(sq[0], col_info(type, (int16_t)sq[2], (uint8_t)(unsigned int)sq[3],
                                             (uint8_t)(unsigned int)sq[4], (u16string)sq[5], nullable,
                                             (unsigned int)sq[7], (u16string)sq[8]));
            }
        }

        return info;
    }

    template
    map<u16string, col_info> TDSCPP get_col_info(tds& n, u16string_view table, u16string_view db);

    template
    map<u16string, col_info> TDSCPP get_col_info(session& n, u16string_view table, u16string_view db);

    void tds_impl::handle_envchange_msg(span<const uint8_t> sp) {
        auto ec = (tds_envchange*)(sp.data() - offsetof(tds_envchange, type));

        switch (ec->type) {
            case tds_envchange_type::database: {
                if (sp.size() < sizeof(tds_envchange_database) - offsetof(tds_envchange_database, header.type)) {
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected at least {}).", sp.size(),
                                          sizeof(tds_envchange_database) - offsetof(tds_envchange_database, header.type));
                }

                auto tedb = (tds_envchange_database*)ec;

                if (tedb->header.length < sizeof(tds_envchange_database) + (tedb->name_len * sizeof(char16_t))) {
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected at least {}).",
                                          tedb->header.length, sizeof(tds_envchange_database) + (tedb->name_len * sizeof(char16_t)));
                }

                db_name = u16string_view{(char16_t*)&tedb[1], tedb->name_len};

                break;
            }

            case tds_envchange_type::begin_trans: {
                if (sp.size() < sizeof(tds_envchange_begin_trans) - offsetof(tds_envchange_begin_trans, header.type))
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected 11).", sp.size());

                auto tebt = (tds_envchange_begin_trans*)ec;

                if (tebt->header.length < offsetof(tds_envchange_begin_trans, new_len))
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected 11).", tebt->header.length);

                if (tebt->new_len != 8)
                    throw formatted_error("Unexpected transaction ID length ({} bytes, expected 8).", tebt->new_len);

                trans_id = tebt->trans_id;

                break;
            }

            case tds_envchange_type::rollback_trans: {
                if (sp.size() < sizeof(tds_envchange_rollback_trans) - offsetof(tds_envchange_rollback_trans, header.type))
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected 11).", sp.size());

                auto tert = (tds_envchange_rollback_trans*)ec;

                if (tert->header.length < offsetof(tds_envchange_rollback_trans, new_len))
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected 11).", tert->header.length);

                trans_id = 0;

                break;
            }

            case tds_envchange_type::commit_trans: {
                if (sp.size() < sizeof(tds_envchange_commit_trans) - offsetof(tds_envchange_begin_trans, header.type))
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected 11).", sp.size());

                auto tect = (tds_envchange_commit_trans*)ec;

                if (tect->header.length < offsetof(tds_envchange_begin_trans, new_len))
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected 11).", tect->header.length);

                trans_id = 0;

                break;
            }

            case tds_envchange_type::packet_size: {
                if (sp.size() < sizeof(tds_envchange_packet_size) - offsetof(tds_envchange_packet_size, header.type)) {
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected at least {}).", sp.size(),
                                          sizeof(tds_envchange_packet_size) - offsetof(tds_envchange_packet_size, header.type));
                }

                auto teps = (tds_envchange_packet_size*)ec;

                if (teps->header.length < sizeof(tds_envchange_packet_size) + (teps->new_len * sizeof(char16_t))) {
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected at least {}).",
                                          teps->header.length, sizeof(tds_envchange_packet_size) + (teps->new_len * sizeof(char16_t)));
                }

                u16string_view s((char16_t*)&teps[1], teps->new_len);
                uint32_t v = 0;

                for (auto c : s) {
                    if (c >= '0' && c <= '9') {
                        v *= 10;
                        v += c - '0';
                    } else
                        throw formatted_error("Server returned invalid packet size \"{}\".", utf16_to_utf8(s));
                }

                packet_size = v;

                break;
            }

            case tds_envchange_type::collation: {
                if (sp.size() < sizeof(tds_envchange_collation) - offsetof(tds_envchange_collation, header.type)) {
                    throw formatted_error("Short ENVCHANGE message ({} bytes, expected at least {}).", sp.size(),
                                          sizeof(tds_envchange_collation) - offsetof(tds_envchange_collation, header.type));
                }

                const auto& tec = *(tds_envchange_collation*)ec;

                if (tec.new_len >= sizeof(collation))
                    coll = tec.collation;
                else
                    coll = {};

                break;
            }

            default:
            break;
        }
    }

    u16string tds::db_name() const {
        return impl->db_name;
    }

    collation tds::current_collation() const {
        return impl->coll;
    }

    trans::trans(tds& conn) : conn(conn) {
        tds_tm_begin msg;

        // FIXME - give transactions names, so that ROLLBACK works as expected?

        msg.header.all_headers.total_size = sizeof(tds_all_headers);
        msg.header.all_headers.size = sizeof(uint32_t) + sizeof(tds_header_trans_desc);
        msg.header.all_headers.trans_desc.type = 2; // transaction descriptor
        msg.header.all_headers.trans_desc.descriptor = conn.impl->trans_id;
        msg.header.all_headers.trans_desc.outstanding = 1;
        msg.header.type = tds_tm_type::TM_BEGIN_XACT;
        msg.isolation_level = 0;
        msg.name_len = 0;

        if (conn.impl->mars_sess)
            conn.impl->mars_sess->send_msg(tds_msg::trans_man_req, span((uint8_t*)&msg, sizeof(msg)));
        else
            conn.impl->sess.send_msg(tds_msg::trans_man_req, span((uint8_t*)&msg, sizeof(msg)));

        enum tds_msg type;
        vector<uint8_t> payload;

        // FIXME - timeout

        if (conn.impl->mars_sess)
            conn.impl->mars_sess->wait_for_msg(type, payload);
        else
            conn.impl->sess.wait_for_msg(type, payload);

        if (type != tds_msg::tabular_result)
            throw formatted_error("Received message type {}, expected tabular_result", (int)type);

        span sp = payload;

        while (!sp.empty()) {
            auto type = (token)sp[0];
            sp = sp.subspan(1);

            switch (type) {
                case token::DONE:
                case token::DONEINPROC:
                case token::DONEPROC:
                    if (sp.size() < sizeof(tds_done_msg))
                        throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), sizeof(tds_done_msg));

                    sp = sp.subspan(sizeof(tds_done_msg));
                break;

                case token::INFO:
                case token::TDS_ERROR:
                case token::ENVCHANGE:
                {
                    if (sp.size() < sizeof(uint16_t))
                        throw formatted_error("Short {} message ({} bytes, expected at least 2).", type, sp.size());

                    auto len = *(uint16_t*)&sp[0];

                    sp = sp.subspan(sizeof(uint16_t));

                    if (sp.size() < len)
                        throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), len);

                    if (type == token::INFO) {
                        if (conn.impl->message_handler)
                            conn.impl->handle_info_msg(sp.subspan(0, len), false);
                    } else if (type == token::TDS_ERROR) {
                        if (conn.impl->message_handler)
                            conn.impl->handle_info_msg(sp.subspan(0, len), true);

                        throw formatted_error("TM_BEGIN_XACT request failed: {}", utf16_to_utf8(extract_message(sp.subspan(0, len))));
                    } else if (type == token::ENVCHANGE)
                        conn.impl->handle_envchange_msg(sp.subspan(0, len));

                    sp = sp.subspan(len);

                    break;
                }

                default:
                    throw formatted_error("Unhandled token type {} in transaction manager response.", type);
            }
        }
    }

    trans::~trans() {
        if (committed)
            return;

        if (conn.impl->trans_id == 0)
            return;

        try {
            tds_tm_rollback msg;

            msg.header.all_headers.total_size = sizeof(tds_all_headers);
            msg.header.all_headers.size = sizeof(uint32_t) + sizeof(tds_header_trans_desc);
            msg.header.all_headers.trans_desc.type = 2; // transaction descriptor
            msg.header.all_headers.trans_desc.descriptor = conn.impl->trans_id;
            msg.header.all_headers.trans_desc.outstanding = 1;
            msg.header.type = tds_tm_type::TM_ROLLBACK_XACT;
            msg.name_len = 0;
            msg.flags = 0;

            if (conn.impl->mars_sess)
                conn.impl->mars_sess->send_msg(tds_msg::trans_man_req, span((uint8_t*)&msg, sizeof(msg)));
            else
                conn.impl->sess.send_msg(tds_msg::trans_man_req, span((uint8_t*)&msg, sizeof(msg)));

            enum tds_msg type;
            vector<uint8_t> payload;

            // FIXME - timeout

            if (conn.impl->mars_sess)
                conn.impl->mars_sess->wait_for_msg(type, payload);
            else
                conn.impl->sess.wait_for_msg(type, payload);

            if (type != tds_msg::tabular_result)
                throw formatted_error("Received message type {}, expected tabular_result", (int)type);

            span sp = payload;

            while (!sp.empty()) {
                auto type = (token)sp[0];
                sp = sp.subspan(1);

                switch (type) {
                    case token::DONE:
                    case token::DONEINPROC:
                    case token::DONEPROC:
                        if (sp.size() < sizeof(tds_done_msg))
                            throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), sizeof(tds_done_msg));

                        sp = sp.subspan(sizeof(tds_done_msg));
                        break;

                    case token::INFO:
                    case token::TDS_ERROR:
                    case token::ENVCHANGE:
                    {
                        if (sp.size() < sizeof(uint16_t))
                            throw formatted_error("Short {} message ({} bytes, expected at least 2).", type, sp.size());

                        auto len = *(uint16_t*)&sp[0];

                        sp = sp.subspan(sizeof(uint16_t));

                        if (sp.size() < len)
                            throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), len);

                        if (type == token::INFO) {
                            if (conn.impl->message_handler) {
                                try {
                                    conn.impl->handle_info_msg(sp.subspan(0, len), false);
                                } catch (...) {
                                }
                            }

                        } else if (type == token::TDS_ERROR) {
                            if (conn.impl->message_handler) {
                                try {
                                    conn.impl->handle_info_msg(sp.subspan(0, len), true);
                                } catch (...) {
                                }
                            }

                            throw formatted_error("TM_ROLLBACK_XACT request failed: {}", utf16_to_utf8(extract_message(sp.subspan(0, len))));
                        } else if (type == token::ENVCHANGE)
                            conn.impl->handle_envchange_msg(sp.subspan(0, len));

                        sp = sp.subspan(len);

                        break;
                    }

                    default:
                        throw formatted_error("Unhandled token type {} in transaction manager response.", type);
                }
            }
        } catch (...) {
            // can't throw in destructor
        }
    }

    void trans::commit() {
        tds_tm_commit msg;

        msg.header.all_headers.total_size = sizeof(tds_all_headers);
        msg.header.all_headers.size = sizeof(uint32_t) + sizeof(tds_header_trans_desc);
        msg.header.all_headers.trans_desc.type = 2; // transaction descriptor
        msg.header.all_headers.trans_desc.descriptor = conn.impl->trans_id;
        msg.header.all_headers.trans_desc.outstanding = 1;
        msg.header.type = tds_tm_type::TM_COMMIT_XACT;
        msg.name_len = 0;
        msg.flags = 0;

        if (conn.impl->mars_sess)
            conn.impl->mars_sess->send_msg(tds_msg::trans_man_req, span((uint8_t*)&msg, sizeof(msg)));
        else
            conn.impl->sess.send_msg(tds_msg::trans_man_req, span((uint8_t*)&msg, sizeof(msg)));

        enum tds_msg type;
        vector<uint8_t> payload;

        // FIXME - timeout

        if (conn.impl->mars_sess)
            conn.impl->mars_sess->wait_for_msg(type, payload);
        else
            conn.impl->sess.wait_for_msg(type, payload);

        if (type != tds_msg::tabular_result)
            throw formatted_error("Received message type {}, expected tabular_result", (int)type);

        span sp = payload;

        while (!sp.empty()) {
            auto type = (token)sp[0];
            sp = sp.subspan(1);

            switch (type) {
                case token::DONE:
                case token::DONEINPROC:
                case token::DONEPROC:
                    if (sp.size() < sizeof(tds_done_msg))
                        throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), sizeof(tds_done_msg));

                    sp = sp.subspan(sizeof(tds_done_msg));
                break;

                case token::INFO:
                case token::TDS_ERROR:
                case token::ENVCHANGE:
                {
                    if (sp.size() < sizeof(uint16_t))
                        throw formatted_error("Short {} message ({} bytes, expected at least 2).", type, sp.size());

                    auto len = *(uint16_t*)&sp[0];

                    sp = sp.subspan(sizeof(uint16_t));

                    if (sp.size() < len)
                        throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), len);

                    if (type == token::INFO) {
                        if (conn.impl->message_handler)
                            conn.impl->handle_info_msg(sp.subspan(0, len), false);
                    } else if (type == token::TDS_ERROR) {
                        if (conn.impl->message_handler)
                            conn.impl->handle_info_msg(sp.subspan(0, len), true);

                        throw formatted_error("TM_COMMIT_XACT request failed: {}", utf16_to_utf8(extract_message(sp.subspan(0, len))));
                    } else if (type == token::ENVCHANGE)
                        conn.impl->handle_envchange_msg(sp.subspan(0, len));

                    sp = sp.subspan(len);

                    break;
                }

                default:
                    throw formatted_error("Unhandled token type {} in transaction manager response.", type);
            }
        }

        committed = true;
    }

    session::session(tds& conn) : conn(conn) {
        if (!conn.impl->mars)
            throw runtime_error("Cannot create session unless MARS is in use.");

        impl = make_unique<smp_session>(*conn.impl.get());
    }

    session::~session() {
        // defined so that unique_ptr destructor gets called
    }

    void TDSCPP to_json(nlohmann::json& j, const value& v) {
        auto type2 = v.type;
        auto val = span(v.val);

        if (v.is_null) {
            j = nlohmann::json(nullptr);
            return;
        }

        if (type2 == sql_type::SQL_VARIANT) {
            type2 = (sql_type)val[0];

            val = val.subspan(1);

            auto propbytes = val[0];

            val = val.subspan(1 + propbytes);
        }

        switch (type2) {
            case sql_type::INTN:
            case sql_type::TINYINT:
            case sql_type::SMALLINT:
            case sql_type::INT:
            case sql_type::BIGINT:
                j = nlohmann::json((int64_t)v);
                break;

            case sql_type::NUMERIC:
            case sql_type::DECIMAL:
            case sql_type::FLOAT:
            case sql_type::REAL:
            case sql_type::MONEYN:
            case sql_type::MONEY:
            case sql_type::SMALLMONEY:
            case sql_type::FLTN:
                j = nlohmann::json((double)v);
                break;

            case sql_type::BITN:
            case sql_type::BIT:
                j = nlohmann::json(val[0] != 0);
                break;

            default:
                j = nlohmann::json((string)v);
        }
    }

    uint16_t tds::spid() const {
        return impl->spid;
    }

    static uint16_t parse_instance_string(string_view s, string_view instance) {
        vector<string_view> instance_list;

        while (!s.empty()) {
            auto ds = s.find(";;");
            string_view t;
            bool this_instance = false;

            if (ds == string::npos) {
                t = s;
                s = "";
            } else {
                t = s.substr(0, ds);
                s = s.substr(ds + 2);
            }

            vector<string_view> el;

            while (!t.empty()) {
                auto sc = t.find(";");

                if (sc == string::npos) {
                    el.emplace_back(t.data(), t.length());
                    break;
                } else {
                    el.emplace_back(t.data(), sc);
                    t = t.substr(sc + 1);
                }
            }

            for (size_t i = 0; i < el.size(); i++) {
                if (el[i] == "InstanceName" && i < el.size() - 1) {
                    this_instance = el[i+1] == instance; // FIXME - should be case-insensitive?

                    if (!this_instance) {
                        instance_list.push_back(el[i+1]);
                        break;
                    }
                } else if (el[i] == "tcp" && i < el.size() - 1 && this_instance) {
                    uint16_t ret;

                    auto fc = from_chars(el[i+1].data(), el[i+1].data() + el[i+1].length() - 1, ret);

                    if (fc.ec == errc::invalid_argument)
                        throw formatted_error("Could not convert port \"{}\" to integer.", el[i+1]);
                    else if (fc.ec == errc::result_out_of_range)
                        throw formatted_error("Port \"{}\" was too large to convert to 16-bit integer.", el[i+1]);

                    return ret;
                }
            }
        }

        auto exc = format("{} not found in instance list (found ", instance);

        for (unsigned int i = 0; i < instance_list.size(); i++) {
            if (i > 0)
                exc += ", ";

            exc += instance_list[i];
        }

        exc += ")";

        throw runtime_error(exc);
    }

    uint16_t get_instance_port(const string& server, string_view instance) {
        struct addrinfo hints;
        struct addrinfo* res;
        struct addrinfo* orig_res;
        uint8_t msg_type;
        uint16_t msg_len, port;
#ifdef _WIN32
        WSADATA wsa_data;
        SOCKET sock = INVALID_SOCKET;
#else
        int sock = 0;
#endif

#ifdef _WIN32

        if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0)
            throw runtime_error("WSAStartup failed.");
#endif

        // connect to port 1434 via UDP

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = /*AF_UNSPEC*/AF_INET;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_protocol = IPPROTO_UDP;

        auto ret = (int)getaddrinfo(server.c_str(), nullptr, &hints, &res);

        if (ret != 0)
            throw formatted_error("getaddrinfo returned {}", ret);

        orig_res = res;
#ifdef _WIN32
        sock = INVALID_SOCKET;
#else
        sock = 0;
#endif

        do {
            sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

#ifdef _WIN32
            if (sock == INVALID_SOCKET)
                continue;
#else
            if (sock < 0)
                continue;
#endif

            if (res->ai_family == AF_INET)
                ((struct sockaddr_in*)res->ai_addr)->sin_port = htons(BROWSER_PORT);
            else if (res->ai_family == AF_INET6)
                ((struct sockaddr_in6*)res->ai_addr)->sin6_port = htons(BROWSER_PORT);
            else {
#ifdef _WIN32
                closesocket(sock);
                sock = INVALID_SOCKET;
#else
                close(sock);
                sock = 0;
#endif
                continue;
            }

            if (::connect(sock, res->ai_addr, (int)res->ai_addrlen) != 0) {
#ifdef _WIN32
                closesocket(sock);
                sock = INVALID_SOCKET;
#else
                close(sock);
                sock = 0;
#endif
                continue;
            }

            break;
        } while ((res = res->ai_next) != nullptr);

        freeaddrinfo(orig_res);

#ifdef _WIN32
        if (sock == INVALID_SOCKET)
            throw formatted_error("Could not connect to {}:{}.", server, BROWSER_PORT);
#else
        if (sock <= 0)
            throw formatted_error("Could not connect to {}:{}.", server, BROWSER_PORT);
#endif

        try {
            ret = (int)send(sock, "\x03", 1, 0);

#ifdef _WIN32
            if (ret < 0)
                throw formatted_error("send failed (error {})", wsa_error_to_string(WSAGetLastError()));
#else
            if (ret < 0)
                throw formatted_error("send failed (error {})", errno_to_string(errno));
#endif

            // FIXME - 1 second timeout

            // wait for reply

            ret = (int)recv(sock, (char*)&msg_type, 1, 0);

#ifdef _WIN32
            if (ret < 0)
                throw formatted_error("recv failed (error {})", wsa_error_to_string(WSAGetLastError()));
#else
            if (ret < 0)
                throw formatted_error("recv failed (error {})", errno_to_string(errno));
#endif

            if (msg_type != 0x05)
                throw formatted_error("response message type was {:02x}, expected 05", msg_type);

            ret = (int)recv(sock, (char*)&msg_len, sizeof(msg_len), 0);

#ifdef _WIN32
            if (ret < 0)
                throw formatted_error("recv failed (error {})", wsa_error_to_string(WSAGetLastError()));
#else
            if (ret < 0)
                throw formatted_error("recv failed (error {})", errno_to_string(errno));
#endif

            string resp(msg_len, 0);

            ret = (int)recv(sock, resp.data(), (int)resp.length(), 0);

#ifdef _WIN32
            if (ret < 0)
                throw formatted_error("recv failed (error {})", wsa_error_to_string(WSAGetLastError()));
#else
            if (ret < 0)
                throw formatted_error("recv failed (error {})", errno_to_string(errno));
#endif

            port = parse_instance_string(resp, instance);
        } catch (...) {
#ifdef _WIN32
            closesocket(sock);
#else
            close(sock);
#endif
            throw;
        }

#ifdef _WIN32
        closesocket(sock);
#else
        close(sock);
#endif

        return port;
    }

    datetime datetime::now() {
        auto n = chrono::system_clock::now();
        auto secs = static_cast<::time_t>(chrono::duration_cast<chrono::seconds>(n.time_since_epoch()).count());
        struct tm t;
        int offset;

        // FIXME - use zoned_time for this, when it's better supported?

#ifdef WIN32
        localtime_s(&t, &secs);

        offset = (int)(_mkgmtime(&t) - secs);
#else
        localtime_r(&secs, &t);

        offset = (int)t.tm_gmtoff;
#endif

        n += chrono::seconds(offset);

        return n;
    }

    datetimeoffset datetimeoffset::now() {
        auto n = chrono::system_clock::now();
        auto secs = static_cast<::time_t>(chrono::duration_cast<chrono::seconds>(n.time_since_epoch()).count());
        struct tm t;
        int offset;

        // FIXME - use zoned_time for this, when it's better supported?

#ifdef WIN32
        localtime_s(&t, &secs);

        offset = (int)(_mkgmtime(&t) - secs);
#else
        localtime_r(&secs, &t);

        offset = (int)t.tm_gmtoff;
#endif

        n += chrono::seconds(offset);

        return {n, (int16_t)(offset / 60)};
    }

    template<unsigned N, typename T>
    constexpr bool test_numeric(T&& t, uint64_t low_part, uint64_t high_part, bool neg) {
        numeric<N> n{t};

        return n.low_part == low_part && n.high_part == high_part && !n.neg == !neg;
    }

    static_assert(test_numeric<0>((int64_t)0, 0, 0, false));
    static_assert(test_numeric<5>((int64_t)0, 0, 0, false));
    static_assert(test_numeric<0>((int64_t)42, 42, 0, false));
    static_assert(test_numeric<5>((int64_t)42, 4200000, 0, false));
    static_assert(test_numeric<18>((int64_t)42, 0x46ddf97976680000, 0x2, false));
    static_assert(test_numeric<19>((int64_t)42, 0xc4abbebea0100000, 0x16, false));
    static_assert(test_numeric<20>((int64_t)42, 0xaeb5737240a00000, 0xe3, false));
    static_assert(test_numeric<21>((int64_t)42, 0xd316827686400000, 0x8e4, false));
    static_assert(test_numeric<22>((int64_t)42, 0x3ee118a13e800000, 0x58f0, false));
    static_assert(test_numeric<23>((int64_t)42, 0x74caf64c71000000, 0x37962, false));
    static_assert(test_numeric<24>((int64_t)42, 0x8fed9efc6a000000, 0x22bdd8, false));
    static_assert(test_numeric<25>((int64_t)42, 0x9f4835dc24000000, 0x15b6a75, false));
    static_assert(test_numeric<26>((int64_t)42, 0x38d21a9968000000, 0xd922898, false));
    static_assert(test_numeric<27>((int64_t)42, 0x383509fe10000000, 0x87b595f2, false));
    static_assert(test_numeric<28>((int64_t)42, 0x321263eca0000000, 0x54d17db76, false));
    static_assert(test_numeric<29>((int64_t)42, 0xf4b7e73e40000000, 0x3502ee929d, false));
    static_assert(test_numeric<0>((int64_t)-17, 17, 0, true));
    static_assert(test_numeric<5>((int64_t)-17, 1700000, 0, true));
    static_assert(test_numeric<0>((uint64_t)0, 0, 0, false));
    static_assert(test_numeric<5>((uint64_t)0, 0, 0, false));
    static_assert(test_numeric<0>((uint64_t)42, 42, 0, false));
    static_assert(test_numeric<5>((uint64_t)42, 4200000, 0, false));
    static_assert(test_numeric<18>((uint64_t)42, 0x46ddf97976680000, 0x2, false));
    static_assert(test_numeric<19>((uint64_t)42, 0xc4abbebea0100000, 0x16, false));
    static_assert(test_numeric<20>((uint64_t)42, 0xaeb5737240a00000, 0xe3, false));
    static_assert(test_numeric<21>((uint64_t)42, 0xd316827686400000, 0x8e4, false));
    static_assert(test_numeric<22>((uint64_t)42, 0x3ee118a13e800000, 0x58f0, false));
    static_assert(test_numeric<23>((uint64_t)42, 0x74caf64c71000000, 0x37962, false));
    static_assert(test_numeric<24>((uint64_t)42, 0x8fed9efc6a000000, 0x22bdd8, false));
    static_assert(test_numeric<25>((uint64_t)42, 0x9f4835dc24000000, 0x15b6a75, false));
    static_assert(test_numeric<26>((uint64_t)42, 0x38d21a9968000000, 0xd922898, false));
    static_assert(test_numeric<27>((uint64_t)42, 0x383509fe10000000, 0x87b595f2, false));
    static_assert(test_numeric<28>((uint64_t)42, 0x321263eca0000000, 0x54d17db76, false));
    static_assert(test_numeric<29>((uint64_t)42, 0xf4b7e73e40000000, 0x3502ee929d, false));
    static_assert(test_numeric<1>((uint64_t)0xffffffffffffffff, 0xfffffffffffffff6, 0x9, false));
    static_assert(test_numeric<0>((int32_t)0, 0, 0, false));
    static_assert(test_numeric<5>((int32_t)0, 0, 0, false));
    static_assert(test_numeric<0>((int32_t)42, 42, 0, false));
    static_assert(test_numeric<5>((int32_t)42, 4200000, 0, false));
    static_assert(test_numeric<0>((int32_t)-17, 17, 0, true));
    static_assert(test_numeric<5>((int32_t)-17, 1700000, 0, true));
    static_assert(test_numeric<0>((uint32_t)0, 0, 0, false));
    static_assert(test_numeric<5>((uint32_t)0, 0, 0, false));
    static_assert(test_numeric<0>((uint32_t)42, 42, 0, false));
    static_assert(test_numeric<5>((uint32_t)42, 4200000, 0, false));
#if 0
    static_assert(test_numeric<0>(0.0, 0, 0, false));
    static_assert(test_numeric<5>(0.0, 0, 0, false));
    static_assert(test_numeric<0>(0x1921fb54442d18p-51, 3, 0, false));
    static_assert(test_numeric<5>(0x1921fb54442d18p-51, 314159, 0, false));
    static_assert(test_numeric<9>(0x1921fb54442d18p-51, 0xbb40e64d, 0, false));
    static_assert(test_numeric<18>(0x1921fb54442d18p-51, 0x2b992ddfa2324c00, 0, false)); // FIXME - slightly wrong
    static_assert(test_numeric<19>(0x1921fb54442d18p-51, 0xb3fbcabc55f6e260, 0x1, false)); // FIXME - probably slightly wrong
    // FIXME - negatives
    // FIXME - floats
#endif
    static_assert(test_numeric<5>(numeric<5>(0), 0, 0, false));
    static_assert(test_numeric<5>(numeric<0>(0), 0, 0, false));
    static_assert(test_numeric<0>(numeric<5>(0), 0, 0, false));
    static_assert(test_numeric<5>(numeric<5>(42), 4200000, 0, false));
    static_assert(test_numeric<5>(numeric<0>(42), 4200000, 0, false));
    static_assert(test_numeric<0>(numeric<5>(42), 42, 0, false));
    static_assert(test_numeric<5>(numeric<5>(-17), 1700000, 0, true));
    static_assert(test_numeric<5>(numeric<0>(-17), 1700000, 0, true));
    static_assert(test_numeric<0>(numeric<5>(-17), 17, 0, true));
    static_assert(test_numeric<18>(numeric<0>(42), 0x46ddf97976680000, 0x2, false));
    static_assert(test_numeric<19>(numeric<0>(42), 0xc4abbebea0100000, 0x16, false));
    static_assert(test_numeric<0>(numeric<18>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<19>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<20>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<21>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<22>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<23>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<24>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<25>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<26>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<27>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<28>(42), 42, 0, false));
    static_assert(test_numeric<0>(numeric<29>(42), 42, 0, false));
    static_assert(test_numeric<18>(numeric<19>(42), 0x46ddf97976680000, 0x2, false));
    static_assert((int64_t)numeric<0>(0) == 0);
    static_assert((int64_t)numeric<5>(0) == 0);
    static_assert((int64_t)numeric<0>(42) == 42);
    static_assert((int64_t)numeric<5>(42) == 42);
    static_assert((int64_t)numeric<0>(-17) == -17);
    static_assert((int64_t)numeric<5>(-17) == -17);
    static_assert((uint64_t)numeric<0>(0) == 0);
    static_assert((uint64_t)numeric<5>(0) == 0);
    static_assert((uint64_t)numeric<0>(42) == 42);
    static_assert((uint64_t)numeric<5>(42) == 42);
    static_assert(numeric<0>(1) == numeric<0>(1));
    static_assert(numeric<0>(-1) < numeric<0>(1));
    static_assert(numeric<0>(1) > numeric<0>(-1));
    static_assert(numeric<0>(7) > numeric<0>(4));
    static_assert(numeric<0>(-7) < numeric<0>(-4));
    static_assert(numeric<21>(1) == numeric<21>(1));
    static_assert(numeric<21>(-1) < numeric<21>(1));
    static_assert(numeric<21>(1) > numeric<21>(-1));
    static_assert(numeric<21>(7) > numeric<21>(4));
    static_assert(numeric<21>(-7) < numeric<21>(-4));
    static_assert(numeric<0>(1) == numeric<1>(1));
    static_assert(numeric<0>(-1) < numeric<1>(1));
    static_assert(numeric<0>(1) > numeric<1>(-1));
    static_assert(numeric<0>(7) > numeric<1>(4));
    static_assert(numeric<0>(-7) < numeric<1>(-4));
    static_assert(numeric<21>(1) == numeric<20>(1));
    static_assert(numeric<21>(-1) < numeric<20>(1));
    static_assert(numeric<21>(1) > numeric<20>(-1));
    static_assert(numeric<21>(7) > numeric<20>(4));
    static_assert(numeric<21>(-7) < numeric<20>(-4));
    static_assert(numeric<1>(1) > numeric<0>(-1));
    static_assert(numeric<1>(-1) < numeric<0>(1));
    static_assert(numeric<1>(4) < numeric<0>(7));
    static_assert(numeric<1>(-4) > numeric<0>(-7));
    static_assert(numeric<20>(1) > numeric<21>(-1));
    static_assert(numeric<20>(-1) < numeric<21>(1));
    static_assert(numeric<20>(4) < numeric<21>(7));
    static_assert(numeric<20>(-4) > numeric<21>(-7));

    constexpr bool test_parse_object_name(string_view s, string_view exp_server, string_view exp_db,
                                          string_view exp_schema, string_view exp_name) {
        auto onp = parse_object_name(s);

        return exp_server == onp.server && exp_db == onp.db && exp_schema == onp.schema && exp_name == onp.name;
    }

    static_assert(test_parse_object_name("server.db.schema.name", "server", "db", "schema", "name"));
    static_assert(test_parse_object_name("server.db.schema.name.extra", "server", "db", "schema", "name"));
    static_assert(test_parse_object_name("[server].[db].[schema].[name]", "[server]", "[db]", "[schema]", "[name]"));
    static_assert(test_parse_object_name("[ser[ver].[d[b].[sch[ema].[na[me]", "[ser[ver]", "[d[b]", "[sch[ema]", "[na[me]"));
    static_assert(test_parse_object_name("[ser.ver].[d.b].[sch.ema].[na.me]", "[ser.ver]", "[d.b]", "[sch.ema]", "[na.me]"));
    static_assert(test_parse_object_name("[ser]]ver].[d]]b].[sch]]ema].[na]]me]", "[ser]]ver]", "[d]]b]", "[sch]]ema]", "[na]]me]"));
    static_assert(test_parse_object_name("db.schema.name", "", "db", "schema", "name"));
    static_assert(test_parse_object_name("[db].[schema].[name]", "", "[db]", "[schema]", "[name]"));
    static_assert(test_parse_object_name("[d[b].[sch[ema].[na[me]", "", "[d[b]", "[sch[ema]", "[na[me]"));
    static_assert(test_parse_object_name("[d.b].[sch.ema].[na.me]", "", "[d.b]", "[sch.ema]", "[na.me]"));
    static_assert(test_parse_object_name("[d]]b].[sch]]ema].[na]]me]", "", "[d]]b]", "[sch]]ema]", "[na]]me]"));
    static_assert(test_parse_object_name("schema.name", "", "", "schema", "name"));
    static_assert(test_parse_object_name("[schema].[name]", "", "", "[schema]", "[name]"));
    static_assert(test_parse_object_name("[sch[ema].[na[me]", "", "", "[sch[ema]", "[na[me]"));
    static_assert(test_parse_object_name("[sch.ema].[na.me]", "", "", "[sch.ema]", "[na.me]"));
    static_assert(test_parse_object_name("[sch]]ema].[na]]me]", "", "", "[sch]]ema]", "[na]]me]"));
    static_assert(test_parse_object_name("name", "", "", "", "name"));
    static_assert(test_parse_object_name("[name]", "", "", "", "[name]"));
    static_assert(test_parse_object_name("[na[me]", "", "", "", "[na[me]"));
    static_assert(test_parse_object_name("[na.me]", "", "", "", "[na.me]"));
    static_assert(test_parse_object_name("[na]]me]", "", "", "", "[na]]me]"));

    constexpr bool test_parse_object_name_u16(u16string_view s, u16string_view exp_server,
                                              u16string_view exp_db, u16string_view exp_schema,
                                              u16string_view exp_name) {
        auto onp = parse_object_name(s);

        return exp_server == onp.server && exp_db == onp.db && exp_schema == onp.schema && exp_name == onp.name;
    }

    static_assert(test_parse_object_name_u16(u"server.db.schema.name", u"server", u"db", u"schema", u"name"));
    static_assert(test_parse_object_name_u16(u"server.db.schema.name.extra", u"server", u"db", u"schema", u"name"));
    static_assert(test_parse_object_name_u16(u"[server].[db].[schema].[name]", u"[server]", u"[db]", u"[schema]", u"[name]"));
    static_assert(test_parse_object_name_u16(u"[ser[ver].[d[b].[sch[ema].[na[me]", u"[ser[ver]", u"[d[b]", u"[sch[ema]", u"[na[me]"));
    static_assert(test_parse_object_name_u16(u"[ser.ver].[d.b].[sch.ema].[na.me]", u"[ser.ver]", u"[d.b]", u"[sch.ema]", u"[na.me]"));
    static_assert(test_parse_object_name_u16(u"[ser]]ver].[d]]b].[sch]]ema].[na]]me]", u"[ser]]ver]", u"[d]]b]", u"[sch]]ema]", u"[na]]me]"));
    static_assert(test_parse_object_name_u16(u"db.schema.name", u"", u"db", u"schema", u"name"));
    static_assert(test_parse_object_name_u16(u"[db].[schema].[name]", u"", u"[db]", u"[schema]", u"[name]"));
    static_assert(test_parse_object_name_u16(u"[d[b].[sch[ema].[na[me]", u"", u"[d[b]", u"[sch[ema]", u"[na[me]"));
    static_assert(test_parse_object_name_u16(u"[d.b].[sch.ema].[na.me]", u"", u"[d.b]", u"[sch.ema]", u"[na.me]"));
    static_assert(test_parse_object_name_u16(u"[d]]b].[sch]]ema].[na]]me]", u"", u"[d]]b]", u"[sch]]ema]", u"[na]]me]"));
    static_assert(test_parse_object_name_u16(u"schema.name", u"", u"", u"schema", u"name"));
    static_assert(test_parse_object_name_u16(u"[schema].[name]", u"", u"", u"[schema]", u"[name]"));
    static_assert(test_parse_object_name_u16(u"[sch[ema].[na[me]", u"", u"", u"[sch[ema]", u"[na[me]"));
    static_assert(test_parse_object_name_u16(u"[sch.ema].[na.me]", u"", u"", u"[sch.ema]", u"[na.me]"));
    static_assert(test_parse_object_name_u16(u"[sch]]ema].[na]]me]", u"", u"", u"[sch]]ema]", u"[na]]me]"));
    static_assert(test_parse_object_name_u16(u"name", u"", u"", u"", u"name"));
    static_assert(test_parse_object_name_u16(u"[name]", u"", u"", u"", u"[name]"));
    static_assert(test_parse_object_name_u16(u"[na[me]", u"", u"", u"", u"[na[me]"));
    static_assert(test_parse_object_name_u16(u"[na.me]", u"", u"", u"", u"[na.me]"));
    static_assert(test_parse_object_name_u16(u"[na]]me]", u"", u"", u"", u"[na]]me]"));
};
