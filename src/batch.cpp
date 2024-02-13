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
    void batch::do_batch(tds& conn, u16string_view q) {
        impl = new batch_impl(conn, q);
    }

    void batch::do_batch(session& sess, u16string_view q) {
        impl = new batch_impl(sess, q);
    }

    batch::~batch() {
        delete impl;
    }

    batch_impl::batch_impl(tds& conn, u16string_view q) : conn(conn) {
        size_t bufsize;

        bufsize = sizeof(tds_all_headers) + (q.length() * sizeof(uint16_t));

        vector<uint8_t> buf(bufsize);

        auto all_headers = (tds_all_headers*)&buf[0];

        all_headers->total_size = sizeof(tds_all_headers);
        all_headers->size = sizeof(uint32_t) + sizeof(tds_header_trans_desc);
        all_headers->trans_desc.type = 2; // transaction descriptor
        all_headers->trans_desc.descriptor = conn.impl->trans_id;
        all_headers->trans_desc.outstanding = 1;

        auto ptr = (char16_t*)&all_headers[1];

        memcpy(ptr, q.data(), q.length() * sizeof(char16_t));

        if (conn.impl->mars_sess)
            conn.impl->mars_sess->send_msg(tds_msg::sql_batch, buf);
        else
            conn.impl->sess.send_msg(tds_msg::sql_batch, buf);

        wait_for_packet();
    }

    batch_impl::batch_impl(session& sess, u16string_view q) : conn(sess.conn) {
        size_t bufsize;

        this->sess.emplace(*sess.impl.get());

        bufsize = sizeof(tds_all_headers) + (q.length() * sizeof(uint16_t));

        vector<uint8_t> buf(bufsize);

        auto all_headers = (tds_all_headers*)&buf[0];

        all_headers->total_size = sizeof(tds_all_headers);
        all_headers->size = sizeof(uint32_t) + sizeof(tds_header_trans_desc);
        all_headers->trans_desc.type = 2; // transaction descriptor
        all_headers->trans_desc.descriptor = conn.impl->trans_id;
        all_headers->trans_desc.outstanding = 1;

        auto ptr = (char16_t*)&all_headers[1];

        memcpy(ptr, q.data(), q.length() * sizeof(char16_t));

        sess.impl->send_msg(tds_msg::sql_batch, buf);

        wait_for_packet();
    }

    batch_impl::~batch_impl() {
        if (finished)
            return;

        try {
            received_attn = false;

            if (sess)
                sess.value().get().send_msg(tds_msg::attention_signal, span<uint8_t>());
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
                    sess.value().get().wait_for_msg(type, payload);
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

    void batch_impl::wait_for_packet() {
        enum tds_msg type;
        vector<uint8_t> payload;
        bool last_packet;

        if (sess)
            sess.value().get().wait_for_msg(type, payload, &last_packet);
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
                    const auto& msg = *(tds_done_msg*)sp.data();

                    if (msg.status & 0x20) // attention
                        received_attn = true;

                    if (conn.impl->count_handler && msg.status & 0x10) // row count valid
                        conn.impl->count_handler(msg.rowcount, msg.curcmd);

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
                            throw formatted_error("SQL batch failed: {}", utf16_to_utf8(extract_message(sp.subspan(0, len))));
                    } else if (type == token::ENVCHANGE)
                        conn.impl->handle_envchange_msg(sp.subspan(0, len));

                    break;
                }

                case token::COLMETADATA:
                {
                    if (sp.size() < 4)
                        throw formatted_error("Short COLMETADATA message ({} bytes, expected at least 4).", sp.size());

                    auto num_columns = *(uint16_t*)&sp[0];

                    cols.clear();
                    cols.reserve(num_columns);

                    if (num_columns == 0)
                        break;

                    auto sp2 = sp;

                    sp2 = sp2.subspan(sizeof(uint16_t));

                    for (unsigned int i = 0; i < num_columns; i++) {
                        if (sp2.size() < sizeof(tds_colmetadata_col))
                            throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), sizeof(tds_colmetadata_col));

                        auto& c = *(tds_colmetadata_col*)&sp2[0];

                        sp2 = sp2.subspan(sizeof(tds_colmetadata_col));

                        cols.emplace_back();

                        auto& col = cols.back();

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
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 3).", sp2.size());

                                col.max_length = (uint8_t)sp2[0];
                                col.precision = (uint8_t)sp2[1];
                                col.scale = (uint8_t)sp2[2];

                                sp2 = sp2.subspan(3);

                                break;

                            case sql_type::SQL_VARIANT:
                                if (sp2.size() < sizeof(uint32_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 4).", sp2.size());

                                col.max_length = *(uint32_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint32_t));
                                break;

                            case sql_type::IMAGE:
                            case sql_type::NTEXT:
                            case sql_type::TEXT:
                            {
                                if (sp2.size() < sizeof(uint32_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 4).", sp2.size());

                                col.max_length = *(uint32_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint32_t));

                                if (c.type == sql_type::TEXT || c.type == sql_type::NTEXT) {
                                    if (sp2.size() < sizeof(collation))
                                        throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 5).", sp2.size());

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
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 2).", sp2.size());

                                col.max_length = *(uint16_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint16_t));

                                // db name

                                if (sp2.size() < sizeof(uint8_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 1).", sp2.size());

                                auto string_len = *(uint8_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint8_t));

                                if (sp2.size() < string_len * sizeof(char16_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), string_len * sizeof(char16_t));

                                sp2 = sp2.subspan(string_len * sizeof(char16_t));

                                // schema name

                                if (sp2.size() < sizeof(uint8_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 1).", sp2.size());

                                string_len = *(uint8_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint8_t));

                                if (sp2.size() < string_len * sizeof(char16_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), string_len * sizeof(char16_t));

                                sp2 = sp2.subspan(string_len * sizeof(char16_t));

                                // type name

                                if (sp2.size() < sizeof(uint8_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 1).", sp2.size());

                                string_len = *(uint8_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint8_t));

                                if (sp2.size() < string_len * sizeof(char16_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), string_len * sizeof(char16_t));

                                sp2 = sp2.subspan(string_len * sizeof(char16_t));

                                // assembly qualified name

                                if (sp2.size() < sizeof(uint16_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least 2).", sp2.size());

                                auto string_len2 = *(uint16_t*)sp2.data();

                                sp2 = sp2.subspan(sizeof(uint16_t));

                                if (sp2.size() < string_len2 * sizeof(char16_t))
                                    throw formatted_error("Short COLMETADATA message ({} bytes left, expected at least {}).", sp2.size(), string_len2 * sizeof(char16_t));

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

                case token::RETURNSTATUS:
                {
                    if (sp.size() < sizeof(int32_t))
                        throw formatted_error("Short RETURNSTATUS message ({} bytes, expected 4).", sp.size());

                    break;
                }

                default:
                    throw formatted_error("Unhandled token type {} while executing SQL batch.", type);
            }
        }

        if (last_packet)
            finished = true;
    }

    bool batch_impl::fetch_row() {
        while (!rows.empty() || !finished) {
            if (!rows.empty()) {
                auto& r = rows.front();

                for (unsigned int i = 0; i < r.size(); i++) {
                    cols[i].is_null = get<1>(r[i]);

                    if (!cols[i].is_null)
                        cols[i].val.swap(get<0>(r[i]));
                }

                rows.pop_front();

                return true;
            }

            if (finished)
                return false;

            wait_for_packet();
        }

        return false;
    }

    bool batch::fetch_row() {
        return impl->fetch_row();
    }

    uint16_t batch::num_columns() const {
        return (uint16_t)impl->cols.size();
    }

    const column& batch::operator[](uint16_t i) const {
        return impl->cols[i];
    }

    column& batch::operator[](uint16_t i) {
        return impl->cols[i];
    }
};
