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
#include <windows.h>
#endif

#include "tdscpp.h"
#include "tdscpp-private.h"
#include <math.h>

#ifndef _WIN32
#define CP_UTF8 65001
#include <unicode/ucnv.h>
#endif

using namespace std;

template<unsigned N>
static void double_to_int(double d, uint8_t* scratch) {
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif
    auto v = *(uint64_t*)&d;
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
    uint64_t exp = v >> 52;
    uint64_t frac = v & 0xfffffffffffff;

    // d is always positive

    frac |= 0x10000000000000; // add implicit leading bit

    // copy frac to buffer

    if constexpr (N < sizeof(uint64_t))
        memcpy(scratch, &frac, N);
    else {
        memcpy(scratch, &frac, sizeof(uint64_t));
        memset(scratch + sizeof(uint64_t), 0, N - sizeof(uint64_t));
    }

    // bitshift buffer according to exp

    while (exp > 0x433) {
        buf_lshift<N>(scratch);
        exp--;
    }

    while (exp < 0x433) {
        buf_rshift<N>(scratch);
        exp++;
    }
}

namespace tds {
    string utf16_to_cp(u16string_view s, unsigned int codepage) {
        string ret;

        if (s.empty())
            return "";

#ifdef _WIN32
        auto len = WideCharToMultiByte(codepage, 0, (const wchar_t*)s.data(), (int)s.length(), nullptr, 0,
                                        nullptr, nullptr);

        if (len == 0)
            throw runtime_error("WideCharToMultiByte 1 failed.");

        ret.resize(len);

        len = WideCharToMultiByte(codepage, 0, (const wchar_t*)s.data(), (int)s.length(), ret.data(), len,
                                    nullptr, nullptr);

        if (len == 0)
            throw runtime_error("WideCharToMultiByte 2 failed.");
#else
        UErrorCode status = U_ZERO_ERROR;
        const char* cp;

        switch (codepage) {
            case 437:
                cp = "ibm-437_P100-1995";
                break;

            case 850:
                cp = "ibm-850_P100-1995";
                break;

            case 874:
                cp = "windows-874-2000";
                break;

            case 932:
                cp = "ibm-942_P12A-1999";
                break;

            case 936:
                cp = "ibm-1386_P100-2001";
                break;

            case 949:
                cp = "windows-949-2000";
                break;

            case 950:
                cp = "windows-950-2000";
                break;

            case 1250:
                cp = "ibm-1250_P100-1995";
                break;

            case 1251:
                cp = "ibm-1251_P100-1995";
                break;

            case 1252:
                cp = "ibm-5348_P100-1997";
                break;

            case 1253:
                cp = "ibm-1253_P100-1995";
                break;

            case 1254:
                cp = "ibm-1254_P100-1995";
                break;

            case 1255:
                cp = "ibm-1255_P100-1995";
                break;

            case 1256:
                cp = "ibm-1256_P110-1997";
                break;

            case 1257:
                cp = "ibm-1257_P100-1995";
                break;

            case 1258:
                cp = "ibm-1258_P100-1997";
                break;

            default:
                throw formatted_error("Could not find ICU name for Windows code page {}.", codepage);
        }

        UConverter* conv = ucnv_open(cp, &status);

        if (U_FAILURE(status))
            throw formatted_error("ucnv_open failed for code page {} ({})", cp, u_errorName(status));

        ret.resize((size_t)UCNV_GET_MAX_BYTES_FOR_STRING(s.length(), ucnv_getMaxCharSize(conv)));

        auto len = ucnv_fromUChars(conv, ret.data(), (int32_t)ret.length(), s.data(), (int32_t)s.length(), &status);

        if (ret.length() > (uint32_t)len)
            ret = ret.substr(0, (size_t)len);

        ucnv_close(conv);
#endif

        return ret;
    }

    u16string cp_to_utf16(string_view s, unsigned int codepage) {
        if (s.empty())
            return u"";

        if (codepage == CP_UTF8)
            return utf8_to_utf16(s);

        u16string us;

#ifdef _WIN32
        auto len = MultiByteToWideChar(codepage, 0, s.data(), (int)s.length(), nullptr, 0);

        if (len == 0)
            throw runtime_error("MultiByteToWideChar 1 failed.");

        us.resize(len);

        len = MultiByteToWideChar(codepage, 0, s.data(), (int)s.length(), (wchar_t*)us.data(), len);

        if (len == 0)
            throw runtime_error("MultiByteToWideChar 2 failed.");
#else
        UErrorCode status = U_ZERO_ERROR;
        const char* cp;

        switch (codepage) {
            case 437:
                cp = "ibm-437_P100-1995";
                break;

            case 850:
                cp = "ibm-850_P100-1995";
                break;

            case 874:
                cp = "windows-874-2000";
                break;

            case 932:
                cp = "ibm-942_P12A-1999";
                break;

            case 936:
                cp = "ibm-1386_P100-2001";
                break;

            case 949:
                cp = "windows-949-2000";
                break;

            case 950:
                cp = "windows-950-2000";
                break;

            case 1250:
                cp = "ibm-1250_P100-1995";
                break;

            case 1251:
                cp = "ibm-1251_P100-1995";
                break;

            case 1252:
                cp = "ibm-5348_P100-1997";
                break;

            case 1253:
                cp = "ibm-1253_P100-1995";
                break;

            case 1254:
                cp = "ibm-1254_P100-1995";
                break;

            case 1255:
                cp = "ibm-1255_P100-1995";
                break;

            case 1256:
                cp = "ibm-1256_P110-1997";
                break;

            case 1257:
                cp = "ibm-1257_P100-1995";
                break;

            case 1258:
                cp = "ibm-1258_P100-1997";
                break;

            default:
                throw formatted_error("Could not find ICU name for Windows code page {}.", codepage);
        }

        UConverter* conv = ucnv_open(cp, &status);

        if (U_FAILURE(status))
            throw formatted_error("ucnv_open failed for code page {} ({})", cp, u_errorName(status));

        us.resize(s.length() * 2); // sic - each input byte might expand to 2 char16_ts

        auto len = ucnv_toUChars(conv, us.data(), (int32_t)us.length() / sizeof(char16_t), s.data(), (int32_t)s.length(), &status);

        if (us.length() > (uint32_t)len)
            us = us.substr(0, (uint32_t)len);

        ucnv_close(conv);
#endif

        return us;
    }

    size_t bcp_row_size(const col_info& col, const value& vv) {
        size_t bufsize;

        switch (col.type) {
            case sql_type::INTN:
                bufsize = 1;

                if (!vv.is_null)
                    bufsize += col.max_length;
                break;

            case sql_type::VARCHAR:
            case sql_type::CHAR:
                bufsize = sizeof(uint16_t);

                if (vv.is_null) {
                    if (col.max_length == -1) // MAX
                        bufsize += sizeof(uint64_t) - sizeof(uint16_t);
                } else {
                    if (col.max_length == -1) // MAX
                        bufsize += sizeof(uint64_t) + sizeof(uint32_t) - sizeof(uint16_t);

                    if ((vv.type == sql_type::VARCHAR || vv.type == sql_type::CHAR) && col.codepage == CP_UTF8) {
                        bufsize += vv.val.size();

                        if (col.max_length == -1 && !vv.val.empty())
                            bufsize += sizeof(uint32_t);
                    } else if (col.codepage == CP_UTF8) {
                        auto s = (string)vv;
                        bufsize += s.length();

                        if (col.max_length == -1 && !s.empty())
                            bufsize += sizeof(uint32_t);
                    } else {
                        auto s = utf16_to_cp((u16string)vv, col.codepage);
                        bufsize += s.length();

                        if (col.max_length == -1 && !s.empty())
                            bufsize += sizeof(uint32_t);
                    }
                }
                break;

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::XML: // changed into NVARCHAR(MAX)
                bufsize = sizeof(uint16_t);

                if (vv.is_null) {
                    if (col.max_length == -1 || col.type == sql_type::XML) // MAX
                        bufsize += sizeof(uint64_t) - sizeof(uint16_t);
                } else {
                    if (col.max_length == -1 || col.type == sql_type::XML) // MAX
                        bufsize += sizeof(uint64_t) + sizeof(uint32_t) - sizeof(uint16_t);

                    if (vv.type == sql_type::NVARCHAR || vv.type == sql_type::NCHAR) {
                        bufsize += vv.val.size();

                        if (col.max_length == -1 && !vv.val.empty())
                            bufsize += sizeof(uint32_t);
                    } else {
                        auto s = (u16string)vv;
                        bufsize += s.length() * sizeof(char16_t);

                        if (col.max_length == -1 && !s.empty())
                            bufsize += sizeof(uint32_t);
                    }
                }
                break;

            case sql_type::VARBINARY:
            case sql_type::BINARY:
            case sql_type::UDT: // changed into VARBINARY(MAX)
                bufsize = sizeof(uint16_t);

                if (vv.is_null) {
                    if (col.max_length == -1 || col.type == sql_type::UDT) // MAX
                        bufsize += sizeof(uint64_t) - sizeof(uint16_t);
                } else {
                    if (col.max_length == -1 || col.type == sql_type::UDT) // MAX
                        bufsize += sizeof(uint64_t) + sizeof(uint32_t) - sizeof(uint16_t);

                    if (vv.type == sql_type::VARBINARY || vv.type == sql_type::BINARY || vv.type == sql_type::UDT) {
                        bufsize += vv.val.size();

                        if ((col.max_length == -1 || col.type == sql_type::UDT) && !vv.val.empty())
                            bufsize += sizeof(uint32_t);
                    } else
                        throw formatted_error("Could not convert {} to {}.", vv.type, col.type);
                }
                break;

            case sql_type::DATE:
                bufsize = 1;

                if (!vv.is_null)
                    bufsize += 3;
                break;

            case sql_type::TIME:
                bufsize = 1;

                if (!vv.is_null) {
                    if (col.scale <= 2)
                        bufsize += 3;
                    else if (col.scale <= 4)
                        bufsize += 4;
                    else
                        bufsize += 5;
                }
                break;

            case sql_type::DATETIME2:
                bufsize = 1;

                if (!vv.is_null) {
                    bufsize += 3;

                    if (col.scale <= 2)
                        bufsize += 3;
                    else if (col.scale <= 4)
                        bufsize += 4;
                    else
                        bufsize += 5;
                }
                break;

            case sql_type::DATETIMEOFFSET:
                bufsize = 1;

                if (!vv.is_null) {
                    bufsize += 5;

                    if (col.scale <= 2)
                        bufsize += 3;
                    else if (col.scale <= 4)
                        bufsize += 4;
                    else
                        bufsize += 5;
                }
                break;

            case sql_type::DATETIME:
                bufsize = sizeof(int32_t) + sizeof(uint32_t);
                break;

            case sql_type::DATETIMN:
                bufsize = 1;

                if (!vv.is_null)
                    bufsize += col.max_length;
                break;

            case sql_type::FLTN:
                bufsize = 1;

                if (!vv.is_null)
                    bufsize += col.max_length;
                break;

            case sql_type::BITN:
                bufsize = 1;

                if (!vv.is_null)
                    bufsize += sizeof(uint8_t);
                break;

            case sql_type::TINYINT:
                bufsize = sizeof(uint8_t);
                break;

            case sql_type::SMALLINT:
                bufsize = sizeof(int16_t);
                break;

            case sql_type::INT:
                bufsize = sizeof(int32_t);
                break;

            case sql_type::BIGINT:
                bufsize = sizeof(int64_t);
                break;

            case sql_type::FLOAT:
                bufsize = sizeof(double);
                break;

            case sql_type::REAL:
                bufsize = sizeof(float);
                break;

            case sql_type::BIT:
                bufsize = sizeof(uint8_t);
                break;

            case sql_type::NUMERIC:
            case sql_type::DECIMAL:
                bufsize = sizeof(uint8_t);

                if (!vv.is_null) {
                    bufsize += sizeof(uint8_t);

                    if (col.precision >= 29)
                        bufsize += 16;
                    else if (col.precision >= 20)
                        bufsize += 12;
                    else if (col.precision >= 10)
                        bufsize += 8;
                    else
                        bufsize += 4;
                }
                break;

            case sql_type::MONEYN:
                bufsize = sizeof(uint8_t);

                if (!vv.is_null)
                    bufsize += col.max_length;
                break;

            case sql_type::MONEY:
                bufsize = sizeof(int64_t);
                break;

            case sql_type::SMALLMONEY:
                bufsize = sizeof(int32_t);
                break;

            case sql_type::DATETIM4:
                bufsize = 4;
                break;

            default:
                throw formatted_error("Unable to send {} in BCP row.", col.type);
        }

        return bufsize;
    }

    static const pair<uint64_t, uint64_t> numeric_limit_vals[] = {
        { 0xa, 0x0 },
        { 0x64, 0x0 },
        { 0x3e8, 0x0 },
        { 0x2710, 0x0 },
        { 0x186a0, 0x0 },
        { 0xf4240, 0x0 },
        { 0x989680, 0x0 },
        { 0x5f5e100, 0x0 },
        { 0x3b9aca00, 0x0 },
        { 0x2540be400, 0x0 },
        { 0x174876e800, 0x0 },
        { 0xe8d4a51000, 0x0 },
        { 0x9184e72a000, 0x0 },
        { 0x5af3107a4000, 0x0 },
        { 0x38d7ea4c68000, 0x0 },
        { 0x2386f26fc10000, 0x0 },
        { 0x16345785d8a0000, 0x0 },
        { 0xde0b6b3a7640000, 0x0 },
        { 0x8ac7230489e80000, 0x0 },
        { 0x6bc75e2d63100000, 0x5 },
        { 0x35c9adc5dea00000, 0x36 },
        { 0x19e0c9bab2400000, 0x21e },
        { 0x2c7e14af6800000, 0x152d },
        { 0x1bcecceda1000000, 0xd3c2 },
        { 0x161401484a000000, 0x84595 },
        { 0xdcc80cd2e4000000, 0x52b7d2 },
        { 0x9fd0803ce8000000, 0x33b2e3c },
        { 0x3e25026110000000, 0x204fce5e },
        { 0x6d7217caa0000000, 0x1431e0fae },
        { 0x4674edea40000000, 0xc9f2c9cd0 },
        { 0xc0914b2680000000, 0x7e37be2022 },
        { 0x85acef8100000000, 0x4ee2d6d415b },
        { 0x38c15b0a00000000, 0x314dc6448d93 },
        { 0x378d8e6400000000, 0x1ed09bead87c0 },
        { 0x2b878fe800000000, 0x13426172c74d82 },
        { 0xb34b9f1000000000, 0xc097ce7bc90715 },
        { 0xf436a000000000, 0x785ee10d5da46d9 },
        { 0x98a224000000000, 0x4b3b4ca85a86c47a },
    };

    static const auto jan1900 = -ymd_to_num({1y, chrono::January, 1d});

    void bcp_row_data(uint8_t*& ptr, const col_info& col, const value& vv, u16string_view col_name) {
        switch (col.type) {
            case sql_type::INTN:
                if (vv.is_null) {
                    *ptr = 0;
                    ptr++;
                } else {
                    *ptr = (uint8_t)col.max_length;
                    ptr++;

                    int64_t n;

                    try {
                        n = (int64_t)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    switch (col.max_length) {
                        case sizeof(uint8_t):
                            if (n < numeric_limits<uint8_t>::min() || n > numeric_limits<uint8_t>::max())
                                throw formatted_error("{} is out of bounds for TINYINT column {}.", n, utf16_to_utf8(col_name));

                            *ptr = (uint8_t)n;
                            ptr++;
                            break;

                        case sizeof(int16_t):
                            if (n < numeric_limits<int16_t>::min() || n > numeric_limits<int16_t>::max())
                                throw formatted_error("{} is out of bounds for SMALLINT column {}.", n, utf16_to_utf8(col_name));

                            *(int16_t*)ptr = (int16_t)n;
                            ptr += sizeof(int16_t);
                            break;

                        case sizeof(int32_t):
                            if (n < numeric_limits<int32_t>::min() || n > numeric_limits<int32_t>::max())
                                throw formatted_error("{} is out of bounds for INT column {}.", n, utf16_to_utf8(col_name));

                            *(int32_t*)ptr = (int32_t)n;
                            ptr += sizeof(int32_t);
                            break;

                        case sizeof(int64_t):
                            *(int64_t*)ptr = n;
                            ptr += sizeof(int64_t);
                            break;

                        default:
                            throw formatted_error("Invalid INTN size {}.", col.max_length);
                    }
                }
                break;

            case sql_type::VARCHAR:
            case sql_type::CHAR:
                if (col.max_length == -1) {
                    if (vv.is_null) {
                        *(uint64_t*)ptr = 0xffffffffffffffff;
                        ptr += sizeof(uint64_t);
                    } else if ((vv.type == sql_type::VARCHAR || vv.type == sql_type::CHAR) && col.codepage == CP_UTF8) {
                        *(uint64_t*)ptr = 0xfffffffffffffffe;
                        ptr += sizeof(uint64_t);

                        if (!vv.val.empty()) {
                            *(uint32_t*)ptr = (uint32_t)vv.val.size();
                            ptr += sizeof(uint32_t);

                            memcpy(ptr, vv.val.data(), vv.val.size());
                            ptr += vv.val.size();
                        }

                        *(uint32_t*)ptr = 0;
                        ptr += sizeof(uint32_t);
                    } else if (col.codepage == CP_UTF8) {
                        auto s = (string)vv;

                        *(uint64_t*)ptr = 0xfffffffffffffffe;
                        ptr += sizeof(uint64_t);

                        if (!s.empty()) {
                            *(uint32_t*)ptr = (uint32_t)s.length();
                            ptr += sizeof(uint32_t);

                            memcpy(ptr, s.data(), s.length());
                            ptr += s.length();
                        }

                        *(uint32_t*)ptr = 0;
                        ptr += sizeof(uint32_t);
                    } else {
                        auto s = utf16_to_cp((u16string)vv, col.codepage);

                        *(uint64_t*)ptr = 0xfffffffffffffffe;
                        ptr += sizeof(uint64_t);

                        if (!s.empty()) {
                            *(uint32_t*)ptr = (uint32_t)s.length();
                            ptr += sizeof(uint32_t);

                            memcpy(ptr, s.data(), s.length());
                            ptr += s.length();
                        }

                        *(uint32_t*)ptr = 0;
                        ptr += sizeof(uint32_t);
                    }
                } else {
                    if (vv.is_null) {
                        *(uint16_t*)ptr = 0xffff;
                        ptr += sizeof(uint16_t);
                    } else if ((vv.type == sql_type::VARCHAR || vv.type == sql_type::CHAR) && col.codepage == CP_UTF8) {
                        if (vv.val.size() > (uint16_t)col.max_length) {
                            throw formatted_error("String \"{}\" too long for column {} (maximum length {}).",
                                                  string_view{(char*)vv.val.data(), vv.val.size()},
                                                  utf16_to_utf8(col_name), col.max_length);
                        }

                        *(uint16_t*)ptr = (uint16_t)vv.val.size();
                        ptr += sizeof(uint16_t);

                        memcpy(ptr, vv.val.data(), vv.val.size());
                        ptr += vv.val.size();
                    } else if (col.codepage == CP_UTF8) {
                        auto s = (string)vv;

                        if (s.length() > (uint16_t)col.max_length)
                            throw formatted_error("String \"{}\" too long for column {} (maximum length {}).", s, utf16_to_utf8(col_name), col.max_length);

                        *(uint16_t*)ptr = (uint16_t)s.length();
                        ptr += sizeof(uint16_t);

                        memcpy(ptr, s.data(), s.length());
                        ptr += s.length();
                    } else {
                        auto s = utf16_to_cp((u16string)vv, col.codepage);

                        if (s.length() > (uint16_t)col.max_length)
                            throw formatted_error("String \"{}\" too long for column {} (maximum length {}).", (string)vv, utf16_to_utf8(col_name), col.max_length);

                        *(uint16_t*)ptr = (uint16_t)s.length();
                        ptr += sizeof(uint16_t);

                        memcpy(ptr, s.data(), s.length());
                        ptr += s.length();
                    }
                }
                break;

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::XML: // changed into NVARCHAR(MAX)
                if (col.max_length == -1 || col.type == sql_type::XML) {
                    if (vv.is_null) {
                        *(uint64_t*)ptr = 0xffffffffffffffff;
                        ptr += sizeof(uint64_t);
                    } else if (vv.type == sql_type::NVARCHAR || vv.type == sql_type::NCHAR) {
                        *(uint64_t*)ptr = 0xfffffffffffffffe;
                        ptr += sizeof(uint64_t);

                        if (!vv.val.empty()) {
                            *(uint32_t*)ptr = (uint32_t)vv.val.size();
                            ptr += sizeof(uint32_t);

                            memcpy(ptr, vv.val.data(), vv.val.size());
                            ptr += vv.val.size();
                        }

                        *(uint32_t*)ptr = 0;
                        ptr += sizeof(uint32_t);
                    } else {
                        auto s = (u16string)vv;

                        *(uint64_t*)ptr = 0xfffffffffffffffe;
                        ptr += sizeof(uint64_t);

                        if (!s.empty()) {
                            *(uint32_t*)ptr = (uint32_t)(s.length() * sizeof(char16_t));
                            ptr += sizeof(uint32_t);

                            memcpy(ptr, s.data(), s.length() * sizeof(char16_t));
                            ptr += s.length() * sizeof(char16_t);
                        }

                        *(uint32_t*)ptr = 0;
                        ptr += sizeof(uint32_t);
                    }
                } else {
                    if (vv.is_null) {
                        *(uint16_t*)ptr = 0xffff;
                        ptr += sizeof(uint16_t);
                    } else if (vv.type == sql_type::NVARCHAR || vv.type == sql_type::NCHAR) {
                        if (vv.val.size() > (uint16_t)col.max_length) {
                            throw formatted_error("String \"{}\" too long for column {} (maximum length {}).",
                                                    utf16_to_utf8(u16string_view((char16_t*)vv.val.data(), vv.val.size() / sizeof(char16_t))),
                                                    utf16_to_utf8(col_name), col.max_length / sizeof(char16_t));
                        }

                        *(uint16_t*)ptr = (uint16_t)vv.val.size();
                        ptr += sizeof(uint16_t);

                        memcpy(ptr, vv.val.data(), vv.val.size());
                        ptr += vv.val.size();
                    } else {
                        auto s = (u16string)vv;

                        if (s.length() > (uint16_t)col.max_length) {
                            throw formatted_error("String \"{}\" too long for column {} (maximum length {}).",
                                                    utf16_to_utf8(u16string_view((char16_t*)s.data(), s.length() / sizeof(char16_t))),
                                                    utf16_to_utf8(col_name), col.max_length / sizeof(char16_t));
                        }

                        *(uint16_t*)ptr = (uint16_t)(s.length() * sizeof(char16_t));
                        ptr += sizeof(uint16_t);

                        memcpy(ptr, s.data(), s.length() * sizeof(char16_t));
                        ptr += s.length() * sizeof(char16_t);
                    }
                }
                break;

            case sql_type::VARBINARY:
            case sql_type::BINARY:
            case sql_type::UDT: // changed into VARBINARY(MAX)
                if (col.max_length == -1 || col.type == sql_type::UDT) {
                    if (vv.is_null) {
                        *(uint64_t*)ptr = 0xffffffffffffffff;
                        ptr += sizeof(uint64_t);
                    } else if (vv.type == sql_type::VARBINARY || vv.type == sql_type::BINARY || vv.type == sql_type::UDT) {
                        *(uint64_t*)ptr = 0xfffffffffffffffe;
                        ptr += sizeof(uint64_t);

                        if (!vv.val.empty()) {
                            *(uint32_t*)ptr = (uint32_t)vv.val.size();
                            ptr += sizeof(uint32_t);

                            memcpy(ptr, vv.val.data(), vv.val.size());
                            ptr += vv.val.size();
                        }

                        *(uint32_t*)ptr = 0;
                        ptr += sizeof(uint32_t);
                    } else
                        throw formatted_error("Could not convert {} to {}.", vv.type, col.type);
                } else {
                    if (vv.is_null) {
                        *(uint16_t*)ptr = 0xffff;
                        ptr += sizeof(uint16_t);
                    } else if (vv.type == sql_type::VARBINARY || vv.type == sql_type::BINARY || vv.type == sql_type::UDT) {
                        if (vv.val.size() > (uint16_t)col.max_length)
                            throw formatted_error("Binary data too long for column {} ({} bytes, maximum {}).", utf16_to_utf8(col_name), vv.val.size(), col.max_length);

                        *(uint16_t*)ptr = (uint16_t)vv.val.size();
                        ptr += sizeof(uint16_t);

                        memcpy(ptr, vv.val.data(), vv.val.size());
                        ptr += vv.val.size();
                    } else
                        throw formatted_error("Could not convert {} to {}.", vv.type, col.type);
                }
                break;

            case sql_type::DATE:
                if (vv.is_null) {
                    *(uint8_t*)ptr = 0;
                    ptr++;
                } else {
                    chrono::year_month_day d;

                    try {
                        d = (chrono::year_month_day)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    uint32_t n = ymd_to_num(d) + jan1900;

                    *(uint8_t*)ptr = 3;
                    ptr++;

                    memcpy(ptr, &n, 3);
                    ptr += 3;
                }
                break;

            case sql_type::TIME:
                if (vv.is_null) {
                    *(uint8_t*)ptr = 0;
                    ptr++;
                } else {
                    uint64_t ticks;

                    try {
                        ticks = time_t(vv).count();
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    for (int j = 0; j < 7 - col.scale; j++) {
                        ticks /= 10;
                    }

                    if (col.scale <= 2) {
                        *(uint8_t*)ptr = 3;
                        ptr++;

                        memcpy(ptr, &ticks, 3);
                        ptr += 3;
                    } else if (col.scale <= 4) {
                        *(uint8_t*)ptr = 4;
                        ptr++;

                        memcpy(ptr, &ticks, 4);
                        ptr += 4;
                    } else {
                        *(uint8_t*)ptr = 5;
                        ptr++;

                        memcpy(ptr, &ticks, 5);
                        ptr += 5;
                    }
                }
                break;

            case sql_type::DATETIME2:
                if (vv.is_null) {
                    *(uint8_t*)ptr = 0;
                    ptr++;
                } else {
                    datetime dt;

                    try {
                        dt = (datetime)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    uint32_t n = ymd_to_num(dt.d) + jan1900;
                    auto ticks = dt.t.count();

                    for (int j = 0; j < 7 - col.scale; j++) {
                        ticks /= 10;
                    }

                    if (col.scale <= 2) {
                        *(uint8_t*)ptr = 6;
                        ptr++;

                        memcpy(ptr, &ticks, 3);
                        ptr += 3;
                    } else if (col.scale <= 4) {
                        *(uint8_t*)ptr = 7;
                        ptr++;

                        memcpy(ptr, &ticks, 4);
                        ptr += 4;
                    } else {
                        *(uint8_t*)ptr = 8;
                        ptr++;

                        memcpy(ptr, &ticks, 5);
                        ptr += 5;
                    }

                    memcpy(ptr, &n, 3);
                    ptr += 3;
                }
                break;

            case sql_type::DATETIMEOFFSET:
                if (vv.is_null) {
                    *(uint8_t*)ptr = 0;
                    ptr++;
                } else {
                    datetimeoffset dto;

                    try {
                        dto = (datetimeoffset)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    uint32_t n = ymd_to_num(dto.d) + jan1900;
                    auto ticks = dto.t.count();

                    for (int j = 0; j < 7 - col.scale; j++) {
                        ticks /= 10;
                    }

                    if (col.scale <= 2) {
                        *(uint8_t*)ptr = 8;
                        ptr++;

                        memcpy(ptr, &ticks, 3);
                        ptr += 3;
                    } else if (col.scale <= 4) {
                        *(uint8_t*)ptr = 9;
                        ptr++;

                        memcpy(ptr, &ticks, 4);
                        ptr += 4;
                    } else {
                        *(uint8_t*)ptr = 10;
                        ptr++;

                        memcpy(ptr, &ticks, 5);
                        ptr += 5;
                    }

                    memcpy(ptr, &n, 3);
                    ptr += 3;

                    *(int16_t*)ptr = dto.offset;
                    ptr += sizeof(int16_t);
                }
                break;

            case sql_type::DATETIME: {
                datetime dt;

                try {
                    dt = (datetime)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                auto ticks = chrono::duration_cast<chrono::duration<int64_t, ratio<1, 300>>>(dt.t);

                *(int32_t*)ptr = ymd_to_num(dt.d);
                ptr += sizeof(int32_t);

                *(uint32_t*)ptr = (uint32_t)ticks.count();
                ptr += sizeof(uint32_t);

                break;
            }

            case sql_type::DATETIMN:
                if (vv.is_null) {
                    *(uint8_t*)ptr = 0;
                    ptr++;
                } else {
                    datetime dt;

                    try {
                        dt = (datetime)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    switch (col.max_length) {
                        case 4: {
                            if (dt.d < num_to_ymd(0))
                                throw formatted_error("Datetime \"{}\" too early for SMALLDATETIME column {}.", dt, utf16_to_utf8(col_name));
                            else if (dt.d > num_to_ymd(numeric_limits<uint16_t>::max()))
                                throw formatted_error("Datetime \"{}\" too late for SMALLDATETIME column {}.", dt, utf16_to_utf8(col_name));

                            *(uint8_t*)ptr = (uint8_t)col.max_length;
                            ptr++;

                            *(uint16_t*)ptr = (uint16_t)ymd_to_num(dt.d);
                            ptr += sizeof(uint16_t);

                            *(uint16_t*)ptr = (uint16_t)chrono::duration_cast<chrono::minutes>(dt.t).count();
                            ptr += sizeof(uint16_t);

                            break;
                        }

                        case 8: {
                            auto dur = chrono::duration_cast<chrono::duration<int64_t, ratio<1, 300>>>(dt.t);

                            *(uint8_t*)ptr = (uint8_t)col.max_length;
                            ptr++;

                            *(int32_t*)ptr = ymd_to_num(dt.d);
                            ptr += sizeof(int32_t);

                            *(uint32_t*)ptr = (uint32_t)dur.count();
                            ptr += sizeof(uint32_t);

                            break;
                        }

                        default:
                            throw formatted_error("DATETIMN has invalid length {}.", col.max_length);
                    }
                }
                break;

            case sql_type::FLTN:
                if (vv.is_null) {
                    *(uint8_t*)ptr = 0;
                    ptr++;
                } else {
                    double d;

                    try {
                        d = (double)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    *(uint8_t*)ptr = (uint8_t)col.max_length;
                    ptr++;

                    switch (col.max_length) {
                        case sizeof(float): {
                            auto f = (float)d;
                            memcpy(ptr, &f, sizeof(float));
                            ptr += sizeof(float);
                            break;
                        }

                        case sizeof(double):
                            memcpy(ptr, &d, sizeof(double));
                            ptr += sizeof(double);
                            break;

                        default:
                            throw formatted_error("FLTN has invalid length {}.", col.max_length);
                    }
                }
                break;

            case sql_type::BITN:
                if (vv.is_null) {
                    *(uint8_t*)ptr = 0;
                    ptr++;
                } else if (vv.type == sql_type::BIT || vv.type == sql_type::BITN) {
                    *(uint8_t*)ptr = sizeof(uint8_t);
                    ptr++;
                    *(uint8_t*)ptr = (uint8_t)vv.val[0];
                    ptr += sizeof(uint8_t);
                } else {
                    int64_t n;

                    try {
                        n = (int64_t)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    *(uint8_t*)ptr = sizeof(uint8_t);
                    ptr++;
                    *(uint8_t*)ptr = n != 0 ? 1 : 0;
                    ptr++;
                }
                break;

            case sql_type::TINYINT: {
                int64_t n;

                try {
                    n = (int64_t)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                if (n < numeric_limits<uint8_t>::min() || n > numeric_limits<uint8_t>::max())
                    throw formatted_error("Value {} is out of bounds for TINYINT column {}.", n, utf16_to_utf8(col_name));

                *(uint8_t*)ptr = (uint8_t)n;
                ptr += sizeof(uint8_t);

                break;
            }

            case sql_type::SMALLINT: {
                int64_t n;

                try {
                    n = (int64_t)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                if (n < numeric_limits<int16_t>::min() || n > numeric_limits<int16_t>::max())
                    throw formatted_error("Value {} is out of bounds for SMALLINT column {}.", n, utf16_to_utf8(col_name));

                *(int32_t*)ptr = (int16_t)n;
                ptr += sizeof(int16_t);

                break;
            }

            case sql_type::INT: {
                int64_t n;

                try {
                    n = (int64_t)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                if (n < numeric_limits<int32_t>::min() || n > numeric_limits<int32_t>::max())
                    throw formatted_error("Value {} is out of bounds for INT column {}.", n, utf16_to_utf8(col_name));

                *(int32_t*)ptr = (int32_t)n;
                ptr += sizeof(int32_t);

                break;
            }

            case sql_type::BIGINT: {
                int64_t n;

                try {
                    n = (int64_t)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                *(int64_t*)ptr = n;
                ptr += sizeof(int64_t);

                break;
            }

            case sql_type::FLOAT: {
                double n;

                try {
                    n = (double)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                *(double*)ptr = n;
                ptr += sizeof(double);

                break;
            }

            case sql_type::REAL: {
                double n;

                try {
                    n = (double)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                *(float*)ptr = (float)n;
                ptr += sizeof(float);

                break;
            }

            case sql_type::BIT: {
                if (vv.type == sql_type::BIT || vv.type == sql_type::BITN) {
                    *(uint8_t*)ptr = (uint8_t)(vv.val[0]);
                    ptr += sizeof(uint8_t);
                } else {
                    int64_t n;

                    try {
                        n = (int64_t)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    *(uint8_t*)ptr = n != 0 ? 1 : 0;
                    ptr += sizeof(uint8_t);
                }

                break;
            }

            case sql_type::NUMERIC:
            case sql_type::DECIMAL:
                if (vv.is_null) {
                    *ptr = 0;
                    ptr++;
                } else {
                    auto type = vv.type;
                    auto precision = vv.precision;
                    auto scale = vv.scale;
                    span data = vv.val;

                    if (type == sql_type::SQL_VARIANT) {
                        type = (sql_type)data[0];

                        data = data.subspan(1);

                        auto propbytes = (uint8_t)data[0];

                        data = data.subspan(1);

                        switch (type) {
                            case sql_type::NUMERIC:
                            case sql_type::DECIMAL:
                                precision = data[0];
                                scale = data[1];
                                break;

                            default:
                                break;
                        }

                        data = data.subspan(propbytes);
                    }

                    switch (type) {
                        case sql_type::NUMERIC:
                        case sql_type::DECIMAL: {
                            const auto& lim = numeric_limit_vals[col.precision - 1];
                            numeric<0> n;

                            if (data.size() >= 9)
                                n.low_part = *(uint64_t*)&data[1];
                            else
                                n.low_part = *(uint32_t*)&data[1];

                            if (data.size() >= 17)
                                n.high_part = *(uint64_t*)&data[1 + sizeof(uint64_t)];
                            else if (data.size() >= 13)
                                n.high_part = *(uint32_t*)&data[1 + sizeof(uint64_t)];
                            else
                                n.high_part = 0;

                            n.neg = data[0] == 0;

                            if (n.high_part > lim.second || (n.high_part == lim.second && n.low_part >= lim.first)) {
                                if (n.neg) {
                                    throw formatted_error("Value {} is too small for NUMERIC({},{}) column {}.", vv, col.precision,
                                                            col.scale, utf16_to_utf8(col_name));
                                } else {
                                    throw formatted_error("Value {} is too large for NUMERIC({},{}) column {}.", vv, col.precision,
                                                            col.scale, utf16_to_utf8(col_name));
                                }
                            }

                            if (precision == col.precision && scale == col.scale) {
                                *ptr = (uint8_t)data.size();
                                ptr++;
                                memcpy(ptr, data.data(), data.size());
                                ptr += data.size();
                                break;
                            }

                            for (auto i = scale; i < col.scale; i++) {
                                n.ten_mult();
                            }

                            for (auto i = col.scale; i < scale; i++) {
                                n.ten_div();
                            }

                            if (col.precision < 10) { // 4 bytes
                                *ptr = 5;
                                ptr++;

                                *ptr = n.neg ? 0 : 1;
                                ptr++;

                                *(uint32_t*)ptr = (uint32_t)n.low_part;
                                ptr += sizeof(uint32_t);
                            } else if (col.precision < 20) { // 8 bytes
                                *ptr = 9;
                                ptr++;

                                *ptr = n.neg ? 0 : 1;
                                ptr++;

                                *(uint64_t*)ptr = n.low_part;
                                ptr += sizeof(uint64_t);
                            } else if (col.precision < 29) { // 12 bytes
                                *ptr = 13;
                                ptr++;

                                *ptr = n.neg ? 0 : 1;
                                ptr++;

                                *(uint64_t*)ptr = n.low_part;
                                ptr += sizeof(uint64_t);

                                *(uint32_t*)ptr = (uint32_t)n.high_part;
                                ptr += sizeof(uint32_t);
                            } else { // 16 bytes
                                *ptr = 17;
                                ptr++;

                                *ptr = n.neg ? 0 : 1;
                                ptr++;

                                *(uint64_t*)ptr = n.low_part;
                                ptr += sizeof(uint64_t);

                                *(uint64_t*)ptr = n.high_part;
                                ptr += sizeof(uint64_t);
                            }

                            break;
                        }

                        default: {
                            bool neg = false;
                            double d;

                            try {
                                d = (double)vv;
                            } catch (const exception& e) {
                                throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                            }

                            if (d < 0) {
                                neg = true;
                                d = -d;
                            }

                            for (unsigned int j = 0; j < col.scale; j++) {
                                d *= 10;
                            }

                            // FIXME - avoid doing pow every time?

                            if (d > pow(10, col.precision)) {
                                if (neg) {
                                    throw formatted_error("Value {} is too small for NUMERIC({},{}) column {}.", vv, col.precision,
                                                            col.scale, utf16_to_utf8(col_name));
                                } else {
                                    throw formatted_error("Value {} is too large for NUMERIC({},{}) column {}.", vv, col.precision,
                                                            col.scale, utf16_to_utf8(col_name));
                                }
                            }

                            if (col.precision < 10) { // 4 bytes
                                *ptr = 5;
                                ptr++;

                                *ptr = neg ? 0 : 1;
                                ptr++;

                                *(uint32_t*)ptr = (uint32_t)d;
                                ptr += sizeof(uint32_t);
                            } else if (col.precision < 20) { // 8 bytes
                                *ptr = 9;
                                ptr++;

                                *ptr = neg ? 0 : 1;
                                ptr++;

                                *(uint64_t*)ptr = (uint64_t)d;
                                ptr += sizeof(uint64_t);
                            } else if (col.precision < 29) { // 12 bytes
                                *ptr = 13;
                                ptr++;

                                *ptr = neg ? 0 : 1;
                                ptr++;

                                double_to_int<12>(d, ptr);
                                ptr += 12;
                            } else { // 16 bytes
                                *ptr = 17;
                                ptr++;

                                *ptr = neg ? 0 : 1;
                                ptr++;

                                double_to_int<16>(d, ptr);
                                ptr += 16;
                            }
                        }
                    }
                }
                break;

            case sql_type::MONEYN: {
                if (vv.is_null) {
                    *ptr = 0;
                    ptr++;
                } else {
                    *ptr = (uint8_t)col.max_length;
                    ptr++;

                    double val;

                    try {
                        val = (double)vv;
                    } catch (const exception& e) {
                        throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                    }

                    val *= 10000.0;

                    switch (col.max_length) {
                        case sizeof(int64_t): {
                            auto v = (int64_t)val;

                            *(int32_t*)ptr = (int32_t)(v >> 32);
                            *(int32_t*)(ptr + sizeof(int32_t)) = (int32_t)(v & 0xffffffff);
                            break;
                        }

                        case sizeof(int32_t):
                            *(int32_t*)ptr = (int32_t)val;
                            break;

                        default:
                            throw formatted_error("MONEYN column {} had invalid size {}.", utf16_to_utf8(col_name), col.max_length);

                    }

                    ptr += col.max_length;
                }

                break;
            }

            case sql_type::MONEY: {
                double val;

                try {
                    val = (double)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                val *= 10000.0;

                auto v = (int64_t)val;

                *(int32_t*)ptr = (int32_t)(v >> 32);
                *(int32_t*)(ptr + sizeof(int32_t)) = (int32_t)(v & 0xffffffff);

                ptr += sizeof(int64_t);

                break;
            }

            case sql_type::SMALLMONEY: {
                double val;

                try {
                    val = (double)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                val *= 10000.0;

                *(int32_t*)ptr = (int32_t)val;
                ptr += sizeof(int32_t);

                break;
            }

            case sql_type::DATETIM4: {
                datetime dt;

                try {
                    dt = (datetime)vv;
                } catch (const exception& e) {
                    throw formatted_error("{} (column {})", e.what(), utf16_to_utf8(col_name));
                }

                if (dt.d < num_to_ymd(0))
                    throw formatted_error("Datetime \"{}\" too early for SMALLDATETIME column {}.", dt, utf16_to_utf8(col_name));
                else if (dt.d > num_to_ymd(numeric_limits<uint16_t>::max()))
                    throw formatted_error("Datetime \"{}\" too late for SMALLDATETIME column {}.", dt, utf16_to_utf8(col_name));

                *(uint16_t*)ptr = (uint16_t)ymd_to_num(dt.d);
                ptr += sizeof(uint16_t);

                *(uint16_t*)ptr = (uint16_t)chrono::duration_cast<chrono::minutes>(dt.t).count();
                ptr += sizeof(uint16_t);

                break;
            }

            default:
                throw formatted_error("Unable to send {} in BCP row.", col.type);
        }
    }

    void tds::bcp_sendmsg(span<const uint8_t> data) {
        if (impl->mars_sess)
            impl->mars_sess->send_msg(tds_msg::bulk_load_data, data);
        else
            impl->sess.send_msg(tds_msg::bulk_load_data, data);

        enum tds_msg type;
        vector<uint8_t> payload;

        if (impl->mars_sess)
            impl->mars_sess->wait_for_msg(type, payload);
        else
            impl->sess.wait_for_msg(type, payload);

        // FIXME - timeout

        if (type != tds_msg::tabular_result)
            throw formatted_error("Received message type {}, expected tabular_result", (int)type);

        span sp = payload;

        while (!sp.empty()) {
            auto type = (token)sp[0];
            sp = sp.subspan(1);

            // FIXME - parse unknowns according to numeric value of type

            switch (type) {
                case token::DONE:
                case token::DONEINPROC:
                case token::DONEPROC:
                    if (sp.size() < sizeof(tds_done_msg))
                        throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), sizeof(tds_done_msg));

                    if (impl->count_handler) {
                        auto msg = (tds_done_msg*)sp.data();

                        if (msg->status & 0x10) // row count valid
                            impl->count_handler(msg->rowcount, msg->curcmd);
                    }

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
                        if (impl->message_handler)
                            impl->handle_info_msg(sp.subspan(0, len), false);
                    } else if (type == token::TDS_ERROR) {
                        if (impl->message_handler)
                            impl->handle_info_msg(sp.subspan(0, len), true);

                        throw formatted_error("BCP failed: {}", utf16_to_utf8(extract_message(sp.subspan(0, len))));
                    } else if (type == token::ENVCHANGE)
                        impl->handle_envchange_msg(sp.subspan(0, len));

                    sp = sp.subspan(len);

                    break;
                }

                default:
                    throw formatted_error("Unhandled token type {} in BCP response.", type);
            }
        }
    }

    void session::bcp_sendmsg(span<const uint8_t> data) {
        impl->send_msg(tds_msg::bulk_load_data, data);

        enum tds_msg type;
        vector<uint8_t> payload;

        impl->wait_for_msg(type, payload);

        // FIXME - timeout

        if (type != tds_msg::tabular_result)
            throw formatted_error("Received message type {}, expected tabular_result", (int)type);

        span sp = payload;

        while (!sp.empty()) {
            auto type = (token)sp[0];
            sp = sp.subspan(1);

            // FIXME - parse unknowns according to numeric value of type

            switch (type) {
                case token::DONE:
                case token::DONEINPROC:
                case token::DONEPROC:
                    if (sp.size() < sizeof(tds_done_msg))
                        throw formatted_error("Short {} message ({} bytes, expected {}).", type, sp.size(), sizeof(tds_done_msg));

                    if (conn.impl->count_handler) {
                        auto msg = (tds_done_msg*)sp.data();

                        if (msg->status & 0x10) // row count valid
                            conn.impl->count_handler(msg->rowcount, msg->curcmd);
                    }

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

                        throw formatted_error("BCP failed: {}", utf16_to_utf8(extract_message(sp.subspan(0, len))));
                    } else if (type == token::ENVCHANGE)
                        conn.impl->handle_envchange_msg(sp.subspan(0, len));

                    sp = sp.subspan(len);

                    break;
                }

                default:
                    throw formatted_error("Unhandled token type {} in BCP response.", type);
            }
        }
    }

    size_t bcp_colmetadata_size(const col_info& col) {
        switch (col.type) {
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
            case sql_type::UNIQUEIDENTIFIER:
            case sql_type::DATE:
                return 0;

            case sql_type::INTN:
            case sql_type::FLTN:
            case sql_type::TIME:
            case sql_type::DATETIME2:
            case sql_type::DATETIMN:
            case sql_type::DATETIMEOFFSET:
            case sql_type::BITN:
            case sql_type::MONEYN:
                return 1;

            case sql_type::VARCHAR:
            case sql_type::NVARCHAR:
            case sql_type::CHAR:
            case sql_type::NCHAR:
            case sql_type::XML: // changed into NVARCHAR(MAX)
                return sizeof(uint16_t) + sizeof(collation);

            case sql_type::VARBINARY:
            case sql_type::BINARY:
            case sql_type::UDT: // changed into VARBINARY(MAX)
                return sizeof(uint16_t);

            case sql_type::DECIMAL:
            case sql_type::NUMERIC:
                return 3;

            default:
                throw formatted_error("Unhandled type {} when creating COLMETADATA token.", col.type);
        }
    }

    void bcp_colmetadata_data(uint8_t*& ptr, const col_info& col, u16string_view name) {
        auto c = (tds_colmetadata_col*)ptr;

        c->user_type = 0;
        c->flags = 8; // read/write

        if (col.nullable)
            c->flags |= 1;

        switch (col.type) {
            case sql_type::XML:
                c->type = sql_type::NVARCHAR;
                break;

            case sql_type::UDT:
                c->type = sql_type::VARBINARY;
                break;

            default:
                c->type = col.type;
                break;
        }

        ptr += sizeof(tds_colmetadata_col);

        switch (col.type) {
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
            case sql_type::UNIQUEIDENTIFIER:
            case sql_type::DATE:
                // nop
                break;

            case sql_type::INTN:
            case sql_type::FLTN:
            case sql_type::BITN:
            case sql_type::MONEYN:
                *(uint8_t*)ptr = (uint8_t)col.max_length;
                ptr++;
                break;

            case sql_type::TIME:
            case sql_type::DATETIME2:
            case sql_type::DATETIMN:
            case sql_type::DATETIMEOFFSET:
                *(uint8_t*)ptr = col.scale;
                ptr++;
                break;

            case sql_type::VARCHAR:
            case sql_type::NVARCHAR:
            case sql_type::CHAR:
            case sql_type::NCHAR:
            case sql_type::XML: // changed into NVARCHAR(MAX)
            {
                *(uint16_t*)ptr = col.type != sql_type::XML ? (uint16_t)col.max_length : 0xffff;
                ptr += sizeof(uint16_t);

                auto c = (collation*)ptr;

                // collation seems to be ignored, depends on what INSERT BULK says

                c->lcid = 0;
                c->ignore_case = 0;
                c->ignore_accent = 0;
                c->ignore_width = 0;
                c->ignore_kana = 0;
                c->binary = 0;
                c->binary2 = 0;
                c->utf8 = 0;
                c->reserved = 0;
                c->version = 0;
                c->sort_id = 0;

                ptr += sizeof(collation);

                break;
            }

            case sql_type::VARBINARY:
            case sql_type::BINARY:
            case sql_type::UDT: // changed into VARBINARY(MAX)
                *(uint16_t*)ptr = col.type != sql_type::UDT ? (uint16_t)col.max_length : 0xffff;
                ptr += sizeof(uint16_t);
                break;

            case sql_type::DECIMAL:
            case sql_type::NUMERIC:
                if (col.precision >= 29)
                    *(uint8_t*)ptr = 17;
                else if (col.precision >= 20)
                    *(uint8_t*)ptr = 13;
                else if (col.precision >= 10)
                    *(uint8_t*)ptr = 9;
                else
                    *(uint8_t*)ptr = 5;

                ptr++;

                *(uint8_t*)ptr = col.precision;
                ptr++;

                *(uint8_t*)ptr = col.scale;
                ptr++;
                break;

            default:
                throw formatted_error("Unhandled type {} when creating COLMETADATA token.", col.type);
        }

        *(uint8_t*)ptr = (uint8_t)name.length();
        ptr++;

        memcpy(ptr, name.data(), name.length() * sizeof(char16_t));
        ptr += name.length() * sizeof(char16_t);
    }
};
