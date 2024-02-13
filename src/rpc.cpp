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

#include "tdscpp.h"
#include "tdscpp-private.h"

using namespace std;

namespace tds {
    void rpc::do_rpc(tds& conn, string_view name) {
        do_rpc(conn, utf8_to_utf16(name));
    }

    void rpc::do_rpc(tds& conn, u16string_view name) {
        size_t bufsize;

        this->name = name;

        bufsize = sizeof(tds_all_headers) + sizeof(uint16_t) + (name.length() * sizeof(uint16_t)) + sizeof(uint16_t);

        for (const auto& p : params) {
            switch (p.type) {
                case sql_type::TINYINT:
                case sql_type::BIT:
                    bufsize += sizeof(tds_param_header) + 1;
                    break;

                case sql_type::SMALLINT:
                    bufsize += sizeof(tds_param_header) + 2;
                    break;

                case sql_type::INT:
                case sql_type::DATETIM4:
                case sql_type::SMALLMONEY:
                case sql_type::REAL:
                    bufsize += sizeof(tds_param_header) + 4;
                    break;

                case sql_type::BIGINT:
                case sql_type::DATETIME:
                case sql_type::MONEY:
                case sql_type::FLOAT:
                    bufsize += sizeof(tds_param_header) + 8;
                    break;

                case sql_type::SQL_NULL:
                    bufsize += sizeof(tds_param_header);
                    break;

                case sql_type::DATETIMN:
                case sql_type::DATE:
                    bufsize += sizeof(tds_param_header) + sizeof(uint8_t) + (p.is_null ? 0 : p.val.size());
                    break;

                case sql_type::UNIQUEIDENTIFIER:
                case sql_type::MONEYN:
                    bufsize += sizeof(tds_param_header) + sizeof(uint8_t) + sizeof(uint8_t) + (p.is_null ? 0 : p.val.size());
                    break;

                case sql_type::INTN:
                case sql_type::FLTN:
                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                case sql_type::BITN:
                    bufsize += sizeof(tds_param_header) + sizeof(uint8_t) + (p.is_null ? 0 : p.val.size()) + sizeof(uint8_t);
                    break;

                case sql_type::NVARCHAR:
                    if (p.is_null)
                        bufsize += sizeof(tds_VARCHAR_param);
                    else if (p.val.size() > 8000) // MAX
                        bufsize += sizeof(tds_VARCHAR_MAX_param) + p.val.size() + sizeof(uint32_t);
                    else
                        bufsize += sizeof(tds_VARCHAR_param) + p.val.size();

                    break;

                case sql_type::VARCHAR:
                    if (p.is_null)
                        bufsize += sizeof(tds_VARCHAR_param);
                    else if (p.coll.utf8 && !conn.impl->coll.utf8) {
                        auto s_len = utf8_to_utf16_len(string_view{(char*)p.val.data(), p.val.size()});

                        if ((s_len * sizeof(char16_t)) > 8000) // MAX
                            bufsize += sizeof(tds_VARCHAR_MAX_param) + (s_len * sizeof(char16_t)) + sizeof(uint32_t);
                        else
                            bufsize += sizeof(tds_VARCHAR_param) + (s_len * sizeof(char16_t));
                    } else if (p.val.size() > 8000) // MAX
                        bufsize += sizeof(tds_VARCHAR_MAX_param) + p.val.size() + sizeof(uint32_t);
                    else
                        bufsize += sizeof(tds_VARCHAR_param) + p.val.size();

                    break;

                case sql_type::VARBINARY:
                    if (!p.is_null && p.val.size() > 8000) // MAX
                        bufsize += sizeof(tds_VARBINARY_MAX_param) + p.val.size() + sizeof(uint32_t);
                    else
                        bufsize += sizeof(tds_VARBINARY_param) + (p.is_null ? 0 : p.val.size());

                    break;

                case sql_type::XML:
                    if (p.is_null)
                        bufsize += offsetof(tds_XML_param, chunk_length);
                    else
                        bufsize += sizeof(tds_XML_param) + p.val.size() + sizeof(uint32_t);
                break;

                case sql_type::NUMERIC:
                case sql_type::DECIMAL:
                    bufsize += sizeof(tds_param_header) + 4;

                    if (!p.is_null)
                        bufsize += p.val.size();
                break;

                case sql_type::IMAGE:
                    bufsize += sizeof(tds_param_header) + sizeof(uint32_t) + sizeof(uint32_t);

                    if (!p.is_null)
                        bufsize += p.val.size();
                break;

                case sql_type::TEXT:
                case sql_type::NTEXT:
                    bufsize += sizeof(tds_param_header) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(collation);

                    if (!p.is_null)
                        bufsize += p.val.size();
                break;

                case sql_type::UDT:
                    if (p.clr_name == u"Microsoft.SqlServer.Types.SqlHierarchyId, Microsoft.SqlServer.Types, Version=11.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91") {
                        static const char16_t schema[] = u"sys";
                        static const char16_t type[] = u"HIERARCHYID";

                        bufsize += sizeof(tds_param_header);

                        bufsize++;
                        bufsize += 1 + sizeof(schema) - sizeof(char16_t);
                        bufsize += 1 + sizeof(type) - sizeof(char16_t);
                        bufsize += sizeof(uint64_t);

                        if (!p.is_null) {
                            bufsize += sizeof(uint32_t);
                            bufsize += p.val.size();
                            bufsize += sizeof(uint32_t);
                        }
                    } else
                        throw formatted_error("Unhandled UDT type {} in RPC params.", utf16_to_utf8(p.clr_name));
                break;

                default:
                    throw formatted_error("Unhandled type {} in RPC params.", p.type);
            }
        }

        vector<uint8_t> buf(bufsize);

        auto all_headers = (tds_all_headers*)&buf[0];

        all_headers->total_size = sizeof(tds_all_headers);
        all_headers->size = sizeof(uint32_t) + sizeof(tds_header_trans_desc);
        all_headers->trans_desc.type = 2; // transaction descriptor
        all_headers->trans_desc.descriptor = conn.impl->trans_id;
        all_headers->trans_desc.outstanding = 1;

        auto ptr = (uint8_t*)&all_headers[1];

        *(uint16_t*)ptr = (uint16_t)name.length();
        ptr += sizeof(uint16_t);

        memcpy(ptr, name.data(), name.length() * sizeof(char16_t));
        ptr += name.length() * sizeof(char16_t);

        *(uint16_t*)ptr = 0; // flags
        ptr += sizeof(uint16_t);

        for (const auto& p : params) {
            auto h = (tds_param_header*)ptr;

            h->name_len = 0;
            h->flags = p.is_output ? 1 : 0;
            h->type = p.type;

            ptr += sizeof(tds_param_header);

            switch (p.type) {
                case sql_type::SQL_NULL:
                case sql_type::TINYINT:
                case sql_type::BIT:
                case sql_type::SMALLINT:
                case sql_type::INT:
                case sql_type::DATETIM4:
                case sql_type::REAL:
                case sql_type::MONEY:
                case sql_type::DATETIME:
                case sql_type::FLOAT:
                case sql_type::SMALLMONEY:
                case sql_type::BIGINT:
                    memcpy(ptr, p.val.data(), p.val.size());

                    ptr += p.val.size();

                    break;

                case sql_type::INTN:
                case sql_type::FLTN:
                case sql_type::BITN:
                    *ptr = (uint8_t)p.val.size();
                    ptr++;

                    if (p.is_null) {
                        *ptr = 0;
                        ptr++;
                    } else {
                        *ptr = (uint8_t)p.val.size();
                        ptr++;
                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();
                    }

                    break;

                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                    *ptr = (uint8_t)p.max_length;
                    ptr++;

                    if (p.is_null) {
                        *ptr = 0;
                        ptr++;
                    } else {
                        *ptr = (uint8_t)p.val.size();
                        ptr++;
                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();
                    }

                    break;

                case sql_type::DATETIMN:
                case sql_type::DATE:
                    if (p.is_null) {
                        *ptr = 0;
                        ptr++;
                    } else {
                        *ptr = (uint8_t)p.val.size();
                        ptr++;
                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();
                    }

                    break;

                case sql_type::UNIQUEIDENTIFIER:
                case sql_type::MONEYN:
                    *ptr = (uint8_t)p.max_length;
                    ptr++;

                    if (p.is_null) {
                        *ptr = 0;
                        ptr++;
                    } else {
                        *ptr = (uint8_t)p.val.size();
                        ptr++;
                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();
                    }

                    break;

                case sql_type::NVARCHAR:
                {
                    auto h2 = (tds_VARCHAR_param*)h;

                    if (p.is_null || p.val.empty())
                        h2->max_length = sizeof(char16_t);
                    else if (p.val.size() > 8000) // MAX
                        h2->max_length = 0xffff;
                    else
                        h2->max_length = (uint16_t)p.val.size();

                    h2->collation = p.coll;

                    if (!p.is_null && p.val.size() > 8000) { // MAX
                        auto h3 = (tds_VARCHAR_MAX_param*)h2;

                        h3->length = h3->chunk_length = (uint32_t)p.val.size();

                        ptr += sizeof(tds_VARCHAR_MAX_param) - sizeof(tds_param_header);

                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();

                        *(uint32_t*)ptr = 0; // last chunk
                        ptr += sizeof(uint32_t);
                    } else {
                        h2->length = (uint16_t)(p.is_null ? 0xffff : p.val.size());

                        ptr += sizeof(tds_VARCHAR_param) - sizeof(tds_param_header);

                        if (!p.is_null && h2->length > 0) {
                            memcpy(ptr, p.val.data(), h2->length);
                            ptr += h2->length;
                        }
                    }

                    break;
                }

                case sql_type::VARCHAR:
                {
                    auto h2 = (tds_VARCHAR_param*)h;
                    string_view sv{(char*)p.val.data(), p.val.size()};
                    u16string tmp;

                    if (!p.is_null && !p.val.empty() && p.coll.utf8 && !conn.impl->coll.utf8) {
                        h->type = sql_type::NVARCHAR;
                        tmp = utf8_to_utf16(string_view{(char*)p.val.data(), p.val.size()});
                        sv = string_view((char*)tmp.data(), tmp.length() * sizeof(char16_t));
                    }

                    if (p.is_null || p.val.empty())
                        h2->max_length = sizeof(char16_t);
                    else if (sv.length() > 8000) // MAX
                        h2->max_length = 0xffff;
                    else
                        h2->max_length = (uint16_t)sv.length();

                    h2->collation = p.coll;

                    if (!p.is_null && sv.length() > 8000) { // MAX
                        auto h3 = (tds_VARCHAR_MAX_param*)h2;

                        h3->length = h3->chunk_length = (uint32_t)sv.length();

                        ptr += sizeof(tds_VARCHAR_MAX_param) - sizeof(tds_param_header);

                        memcpy(ptr, sv.data(), sv.length());
                        ptr += sv.length();

                        *(uint32_t*)ptr = 0; // last chunk
                        ptr += sizeof(uint32_t);
                    } else {
                        h2->length = (uint16_t)(p.is_null ? 0xffff : sv.length());

                        ptr += sizeof(tds_VARCHAR_param) - sizeof(tds_param_header);

                        if (!p.is_null) {
                            memcpy(ptr, sv.data(), h2->length);
                            ptr += h2->length;
                        }
                    }

                    break;
                }

                case sql_type::VARBINARY: {
                    auto h2 = (tds_VARBINARY_param*)h;

                    if (p.is_null || p.val.empty())
                        h2->max_length = 1;
                    else if (p.val.size() > 8000) // MAX
                        h2->max_length = 0xffff;
                    else
                        h2->max_length = (uint16_t)p.val.size();

                    if (!p.is_null && p.val.size() > 8000) { // MAX
                        auto h3 = (tds_VARBINARY_MAX_param*)h2;

                        h3->length = h3->chunk_length = (uint32_t)p.val.size();

                        ptr += sizeof(tds_VARBINARY_MAX_param) - sizeof(tds_param_header);

                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();

                        *(uint32_t*)ptr = 0; // last chunk
                        ptr += sizeof(uint32_t);
                    } else {
                        h2->length = (uint16_t)(p.is_null ? 0xffff : p.val.size());

                        ptr += sizeof(tds_VARBINARY_param) - sizeof(tds_param_header);

                        if (!p.is_null) {
                            memcpy(ptr, p.val.data(), h2->length);
                            ptr += h2->length;
                        }
                    }

                    break;
                }

                case sql_type::XML: {
                    auto h2 = (tds_XML_param*)h;

                    h2->flags = 0;

                    if (p.is_null)
                        h2->length = 0xffffffffffffffff;
                    else {
                        h2->length = h2->chunk_length = (uint32_t)p.val.size();

                        ptr += sizeof(tds_XML_param) - sizeof(tds_param_header);

                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();

                        *(uint32_t*)ptr = 0; // last chunk
                        ptr += sizeof(uint32_t);
                    }

                    break;
                }

                case sql_type::NUMERIC:
                case sql_type::DECIMAL:
                    *ptr = (uint8_t)p.max_length; ptr++;
                    *ptr = p.precision; ptr++;
                    *ptr = p.scale; ptr++;

                    if (p.is_null) {
                        *ptr = 0;
                        ptr++;
                    } else {
                        *ptr = (uint8_t)p.val.size();
                        ptr++;

                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();
                    }
                break;

                case sql_type::IMAGE:
                    *(uint32_t*)ptr = 0x7fffffff;
                    ptr += sizeof(uint32_t);

                    if (p.is_null) {
                        *(uint32_t*)ptr = 0xffffffff;
                        ptr += sizeof(uint32_t);
                    } else {
                        *(uint32_t*)ptr = (uint32_t)p.val.size();
                        ptr += sizeof(uint32_t);

                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();
                    }
                break;

                case sql_type::TEXT:
                case sql_type::NTEXT:
                {
                    *(uint32_t*)ptr = 0x7fffffff;
                    ptr += sizeof(uint32_t);

                    auto& col = *(collation*)ptr;

                    col = p.coll;

                    ptr += sizeof(collation);

                    if (p.is_null) {
                        *(uint32_t*)ptr = 0xffffffff;
                        ptr += sizeof(uint32_t);
                    } else {
                        *(uint32_t*)ptr = (uint32_t)p.val.size();
                        ptr += sizeof(uint32_t);

                        memcpy(ptr, p.val.data(), p.val.size());
                        ptr += p.val.size();
                    }

                    break;
                }

                case sql_type::UDT:
                    if (p.clr_name == u"Microsoft.SqlServer.Types.SqlHierarchyId, Microsoft.SqlServer.Types, Version=11.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91") {
                        static const char16_t schema[] = u"sys";
                        static const char16_t type[] = u"HIERARCHYID";

                        // DB name
                        *ptr = 0;
                        ptr++;

                        // schema name
                        *ptr = (sizeof(schema) / sizeof(char16_t)) - 1;
                        ptr++;
                        memcpy(ptr, schema, sizeof(schema) - sizeof(char16_t));
                        ptr += sizeof(schema) - sizeof(char16_t);

                        // type name
                        *ptr = (sizeof(type) / sizeof(char16_t)) - 1;
                        ptr++;
                        memcpy(ptr, type, sizeof(type) - sizeof(char16_t));
                        ptr += sizeof(type) - sizeof(char16_t);

                        if (p.is_null) {
                            *(uint64_t*)ptr = 0xffffffffffffffff;
                            ptr += sizeof(uint64_t);
                        } else {
                            *(uint64_t*)ptr = p.val.size();
                            ptr += sizeof(uint64_t);
                            *(uint32_t*)ptr = (uint32_t)p.val.size();
                            ptr += sizeof(uint32_t);

                            memcpy(ptr, p.val.data(), p.val.size());
                            ptr += p.val.size();

                            *(uint32_t*)ptr = 0;
                            ptr += sizeof(uint32_t);
                        }
                    } else
                        throw formatted_error("Unhandled UDT type {} in RPC params.", utf16_to_utf8(p.clr_name));
                break;

                default:
                    throw formatted_error("Unhandled type {} in RPC params.", p.type);
            }
        }

        if (sess)
            sess->get().send_msg(tds_msg::rpc, buf);
        else if (conn.impl->mars_sess)
            conn.impl->mars_sess->send_msg(tds_msg::rpc, buf);
        else
            conn.impl->sess.send_msg(tds_msg::rpc, buf);

        wait_for_packet();
    }

    void rpc::do_rpc(session& sess, string_view name) {
        do_rpc(sess, utf8_to_utf16(name));
    }

    void rpc::do_rpc(session& sess, u16string_view name) {
        this->sess.emplace(*sess.impl.get());

        do_rpc(sess.conn, name);
    }

    rpc::~rpc() {
        if (finished)
            return;

        try {
            received_attn = false;

            if (sess)
                sess->get().send_msg(tds_msg::attention_signal, span<uint8_t>());
            else if (conn.impl->mars_sess)
                conn.impl->mars_sess->send_msg(tds_msg::attention_signal, span<uint8_t>());
            else
                conn.impl->sess.send_msg(tds_msg::attention_signal, span<uint8_t>());

            while (!finished) {
                wait_for_packet();
            }

            // wait for attention acknowledgement

            bool ack = received_attn;

            while (!ack) {
                enum tds_msg type;
                vector<uint8_t> payload;

                if (sess)
                    sess->get().wait_for_msg(type, payload);
                else if (conn.impl->mars_sess)
                    conn.impl->mars_sess->wait_for_msg(type, payload);
                else
                    conn.impl->sess.wait_for_msg(type, payload);

                // FIXME - timeout

                if (type != tds_msg::tabular_result)
                    continue;

                parse_tokens(payload, tokens, buf_columns);

                vector<uint8_t> t;

                while (!tokens.empty()) {
                    t.swap(tokens.front());
                    tokens.pop_front();

                    auto type = (token)t[0];

                    switch (type) {
                        case token::DONE:
                        case token::DONEINPROC:
                        case token::DONEPROC: {
                            auto m = (tds_done_msg*)&t[1];

                            if (m->status & 0x20)
                                ack = true;

                            break;
                        }

                        default:
                            break;
                    }
                }
            }
        } catch (...) {
            // can't throw in destructor
        }
    }

    void rpc::wait_for_packet() {
        enum tds_msg type;
        vector<uint8_t> payload;
        bool last_packet;

        if (sess)
            sess->get().wait_for_msg(type, payload, &last_packet);
        else if (conn.impl->mars_sess)
            conn.impl->mars_sess->wait_for_msg(type, payload, &last_packet);
        else
            conn.impl->sess.wait_for_msg(type, payload, &last_packet);

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

        vector<uint8_t> t;

        while (!tokens.empty()) {
            t.swap(tokens.front());
            tokens.pop_front();

            span<const uint8_t> sp = t;

            auto type = (token)sp[0];
            sp = sp.subspan(1);

            switch (type) {
                case token::DONE:
                case token::DONEINPROC:
                case token::DONEPROC:
                {
                    if (sp.size() < sizeof(tds_done_msg))
                        throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), sizeof(tds_done_msg));

                    const auto& msg = *(tds_done_msg*)sp.data();

                    if (msg.status & 0x20) // attention
                        received_attn = true;

                    if (conn.impl->count_handler && msg.status & 0x10) // row count valid
                        conn.impl->count_handler(msg.rowcount, msg.curcmd);

                    // FIXME - handle RPCs that return multiple row sets?
                    break;
                }

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
                        else
                            throw formatted_error("RPC {} failed: {}", utf16_to_utf8(name), utf16_to_utf8(extract_message(sp.subspan(0, len))));
                    } else if (type == token::ENVCHANGE)
                        conn.impl->handle_envchange_msg(sp.subspan(0, len));

                    break;
                }

                case token::RETURNSTATUS:
                {
                    if (sp.size() < sizeof(int32_t))
                        throw formatted_error("Short RETURNSTATUS message ({} bytes, expected 4).", sp.size());

                    return_status = *(int32_t*)&sp[0];

                    break;
                }

                case token::COLMETADATA:
                {
                    if (sp.size() < 4)
                        throw formatted_error("Short COLMETADATA message ({} bytes, expected at least 4).", sp.size());

                    auto num_columns = *(uint16_t*)&sp[0];

                    if (num_columns == 0)
                        break;

                    cols.clear();
                    cols.reserve(num_columns);

                    auto sp2 = sp;

                    sp2 = sp2.subspan(sizeof(uint16_t));

                    for (unsigned int i = 0; i < num_columns; i++) {
                        if (sp2.size() < sizeof(tds_colmetadata_col))
                            throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), sizeof(tds_colmetadata_col));

                        auto& c = *(tds_colmetadata_col*)&sp2[0];

                        sp2 = sp2.subspan(sizeof(tds_colmetadata_col));

                        cols.emplace_back();

                        auto& col = cols.back();

                        col.nullable = c.flags & 1;

                        col.type = c.type;

                        switch (c.type) {
                            case sql_type::SQL_NULL:
                            case sql_type::TINYINT:
                            case sql_type::BIT:
                            case sql_type::SMALLINT:
                            case sql_type::INT:
                            case sql_type::DATETIM4:
                            case sql_type::REAL:
                            case sql_type::MONEY:
                            case sql_type::DATETIME:
                            case sql_type::FLOAT:
                            case sql_type::SMALLMONEY:
                            case sql_type::BIGINT:
                            case sql_type::DATE:
                                // nop
                                break;

                            case sql_type::INTN:
                            case sql_type::FLTN:
                            case sql_type::TIME:
                            case sql_type::DATETIME2:
                            case sql_type::DATETIMN:
                            case sql_type::DATETIMEOFFSET:
                            case sql_type::BITN:
                            case sql_type::MONEYN:
                            case sql_type::UNIQUEIDENTIFIER:
                                if (sp2.size() < sizeof(uint8_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 1).", sp2.size());

                                col.max_length = *(uint8_t*)sp2.data();

                                sp2 = sp2.subspan(1);
                                break;

                            case sql_type::VARCHAR:
                            case sql_type::NVARCHAR:
                            case sql_type::CHAR:
                            case sql_type::NCHAR: {
                                if (sp2.size() < sizeof(uint16_t) + sizeof(collation))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), sizeof(uint16_t) + sizeof(collation));

                                col.max_length = *(uint16_t*)sp2.data();

                                col.coll = *(collation*)(sp2.data() + sizeof(uint16_t));

                                sp2 = sp2.subspan(sizeof(uint16_t) + sizeof(collation));
                                break;
                            }

                            case sql_type::VARBINARY:
                            case sql_type::BINARY:
                                if (sp2.size() < sizeof(uint16_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), sizeof(uint16_t));

                                col.max_length = *(uint16_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint16_t));
                                break;

                            case sql_type::XML:
                                if (sp2.size() < sizeof(uint8_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 1).", sp2.size());

                                sp2 = sp2.subspan(sizeof(uint8_t));
                                break;

                            case sql_type::DECIMAL:
                            case sql_type::NUMERIC:
                                if (sp2.size() < 3)
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 3).", sp2.size(), 3);

                                col.max_length = (uint8_t)sp2[0];
                                col.precision = (uint8_t)sp2[1];
                                col.scale = (uint8_t)sp2[2];

                                sp2 = sp2.subspan(3);

                                break;

                            case sql_type::SQL_VARIANT:
                                if (sp2.size() < sizeof(uint32_t))
                                    return;

                                col.max_length = *(uint32_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint32_t));
                                break;

                            case sql_type::IMAGE:
                            case sql_type::TEXT:
                            case sql_type::NTEXT:
                            {
                                if (sp2.size() < sizeof(uint32_t))
                                    return;

                                col.max_length = *(uint32_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint32_t));

                                if (c.type == sql_type::TEXT || c.type == sql_type::NTEXT) {
                                    if (sp2.size() < sizeof(collation))
                                        return;

                                    sp2 = sp2.subspan(sizeof(collation));
                                }

                                if (sp2.size() < 1)
                                    return;

                                auto num_parts = (uint8_t)sp2[0];

                                sp2 = sp2.subspan(1);

                                for (uint8_t j = 0; j < num_parts; j++) {
                                    if (sp2.size() < sizeof(uint16_t))
                                        return;

                                    auto partlen = *(uint16_t*)sp2.data();

                                    sp2 = sp2.subspan(sizeof(uint16_t));

                                    if (sp2.size() < partlen * sizeof(char16_t))
                                        return;

                                    sp2 = sp2.subspan(partlen * sizeof(char16_t));
                                }

                                break;
                            }

                            case sql_type::UDT:
                            {
                                if (sp2.size() < sizeof(uint16_t))
                                    return;

                                col.max_length = *(uint16_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint16_t));

                                // db name

                                if (sp2.size() < sizeof(uint8_t))
                                    return;

                                auto string_len = *(uint8_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint8_t));

                                if (sp2.size() < string_len * sizeof(char16_t))
                                    return;

                                sp2 = sp2.subspan(string_len * sizeof(char16_t));

                                // schema name

                                if (sp2.size() < sizeof(uint8_t))
                                    return;

                                string_len = *(uint8_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint8_t));

                                if (sp2.size() < string_len * sizeof(char16_t))
                                    return;

                                sp2 = sp2.subspan(string_len * sizeof(char16_t));

                                // type name

                                if (sp2.size() < sizeof(uint8_t))
                                    return;

                                string_len = *(uint8_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint8_t));

                                if (sp2.size() < string_len * sizeof(char16_t))
                                    return;

                                sp2 = sp2.subspan(string_len * sizeof(char16_t));

                                // assembly qualified name

                                if (sp2.size() < sizeof(uint16_t))
                                    return;

                                auto string_len2 = *(uint16_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint16_t));

                                if (sp2.size() < string_len2 * sizeof(char16_t))
                                    return;

                                col.clr_name.assign((uint16_t*)sp2.data(), (uint16_t*)sp2.data() + string_len2);

                                sp2 = sp2.subspan(string_len2 * sizeof(char16_t));

                                break;
                            }

                            default:
                                throw formatted_error("Unhandled type {} in COLMETADATA message.", c.type);
                        }

                        if (sp2.size() < 1)
                            throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 1).", sp2.size());

                        auto name_len = *(uint8_t*)&sp2[0];

                        sp2 = sp2.subspan(1);

                        if (sp2.size() < name_len * sizeof(char16_t))
                            throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), name_len * sizeof(char16_t));

                        col.name = u16string_view((char16_t*)sp2.data(), name_len);

                        sp2 = sp2.subspan(name_len * sizeof(char16_t));
                    }

                    break;
                }

                case token::RETURNVALUE:
                {
                    auto h = (tds_return_value*)&sp[0];

                    if (sp.size() < sizeof(tds_return_value))
                        throw formatted_error("Short RETURNVALUE message ({} bytes, expected at least {}).", sp.size(), sizeof(tds_return_value));

                    // FIXME - param name

                    if (is_byte_len_type(h->type)) {
                        uint8_t len;

                        if (sp.size() < sizeof(tds_return_value) + 2)
                            throw formatted_error("Short RETURNVALUE message ({} bytes, expected at least {}).", sp.size(), sizeof(tds_return_value) + 2);

                        len = *((uint8_t*)&sp[0] + sizeof(tds_return_value) + 1);

                        if (sp.size() < sizeof(tds_return_value) + 2 + len)
                            throw formatted_error("Short RETURNVALUE message ({} bytes, expected {}).", sp.size(), sizeof(tds_return_value) + 2 + len);

                        if (output_params.count(h->param_ordinal) != 0) {
                            value& out = *output_params.at(h->param_ordinal);

                            if (len == 0)
                                out.is_null = true;
                            else {
                                out.is_null = false;

                                // FIXME - make sure not unexpected size?

                                out.val.resize(len);
                                memcpy(out.val.data(), (uint8_t*)&sp[0] + sizeof(tds_return_value) + 2, len);
                            }
                        }
                    } else
                        throw formatted_error("Unhandled type {} in RETURNVALUE message.", h->type);

                    break;
                }

                case token::ROW:
                {
                    rows.emplace_back();

                    auto& row = rows.back();

                    row.resize(cols.size());

                    for (unsigned int i = 0; i < row.size(); i++) {
                        auto& col = row[i];

                        handle_row_col(get<0>(col), get<1>(col), cols[i].type, cols[i].max_length, sp);
                    }

                    break;
                }

                case token::NBCROW:
                    handle_nbcrow(sp, cols, rows);
                    break;

                case token::ORDER:
                {
                    if (sp.size() < sizeof(uint16_t))
                        throw formatted_error("Short ORDER message ({} bytes, expected at least {}).", sp.size(), sizeof(uint16_t));

                    auto len = *(uint16_t*)sp.data();
                    sp = sp.subspan(sizeof(uint16_t));

                    if (sp.size() < len)
                        throw formatted_error("Short ORDER message ({} bytes, expected {}).", sp.size(), len);

                    break;
                }

                default:
                    throw formatted_error("Unhandled token type {} while executing RPC.", type);
            }
        }

        if (last_packet)
            finished = true;
    }

    bool rpc::fetch_row_no_wait() {
        if (rows.empty())
            return false;

        auto& r = rows.front();

        for (unsigned int i = 0; i < r.size(); i++) {
            cols[i].is_null = get<1>(r[i]);

            if (!cols[i].is_null)
                cols[i].val.swap(get<0>(r[i]));
        }

        rows.pop_front();

        return true;
    }

    bool rpc::fetch_row() {
        while (!rows.empty() || !finished) {
            if (fetch_row_no_wait())
                return true;

            if (finished)
                return false;

            wait_for_packet();
        }

        return false;
    }

    uint16_t rpc::num_columns() const {
        return (uint16_t)cols.size();
    }

    const column& rpc::operator[](uint16_t i) const {
        return cols[i];
    }

    column& rpc::operator[](uint16_t i) {
        return cols[i];
    }
};
