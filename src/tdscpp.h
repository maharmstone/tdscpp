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

#pragma once

#include <string>
#include <list>
#include <functional>
#include <optional>
#include <vector>
#include <map>
#include <span>
#include <ranges>
#include <chrono>
#include <array>
#include <time.h>
#include <string.h>
#include <nlohmann/json_fwd.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251)
#endif

#ifdef _WIN32

#ifdef TDSCPP_EXPORT
#define TDSCPP __declspec(dllexport)
#elif !defined(TDSCPP_STATIC)
#define TDSCPP __declspec(dllimport)
#else
#define TDSCPP
#endif

#else

#ifdef TDSCPP_EXPORT
#define TDSCPP __attribute__ ((visibility ("default")))
#elif !defined(TDSCPP_STATIC)
#define TDSCPP __attribute__ ((dllimport))
#else
#define TDSCPP
#endif

#endif

#ifdef __GNUC__
#define WARN_UNUSED __attribute__ ((warn_unused))
#else
#define WARN_UNUSED
#endif

namespace tds {
    enum class sql_type : uint8_t {
        SQL_NULL = 0x1F,
        IMAGE = 0x22,
        TEXT = 0x23,
        UNIQUEIDENTIFIER = 0x24,
        INTN = 0x26,
        DATE = 0x28,
        TIME = 0x29,
        DATETIME2 = 0x2A,
        DATETIMEOFFSET = 0x2B,
        TINYINT = 0x30,
        BIT = 0x32,
        SMALLINT = 0x34,
        INT = 0x38,
        DATETIM4 = 0x3A,
        REAL = 0x3B,
        MONEY = 0x3C,
        DATETIME = 0x3D,
        FLOAT = 0x3E,
        SQL_VARIANT = 0x62,
        NTEXT = 0x63,
        BITN = 0x68,
        DECIMAL = 0x6A,
        NUMERIC = 0x6C,
        FLTN = 0x6D,
        MONEYN = 0x6E,
        DATETIMN = 0x6F,
        SMALLMONEY = 0x7A,
        BIGINT = 0x7F,
        VARBINARY = 0xA5,
        VARCHAR = 0xA7,
        BINARY = 0xAD,
        CHAR = 0xAF,
        NVARCHAR = 0xE7,
        NCHAR = 0xEF,
        UDT = 0xF0,
        XML = 0xF1,
    };

    enum class token : uint8_t {
        OFFSET = 0x78,
        RETURNSTATUS = 0x79,
        COLMETADATA = 0x81,
        ALTMETADATA = 0x88,
        DATACLASSIFICATION = 0xa3,
        TABNAME = 0xa4,
        COLINFO = 0xa5,
        ORDER = 0xa9,
        TDS_ERROR = 0xaa,
        INFO = 0xab,
        RETURNVALUE = 0xac,
        LOGINACK = 0xad,
        FEATUREEXTACK = 0xae,
        ROW = 0xd1,
        NBCROW = 0xd2,
        ALTROW = 0xd3,
        ENVCHANGE = 0xe3,
        SESSIONSTATE = 0xe4,
        SSPI = 0xed,
        FEDAUTHINFO = 0xee,
        DONE = 0xfd,
        DONEPROC = 0xfe,
        DONEINPROC = 0xff
    };

#pragma pack(push, 1)

    struct tds_colmetadata_col {
        uint32_t user_type;
        uint16_t flags;
        tds::sql_type type;
    };

#pragma pack(pop)

    static_assert(sizeof(tds_colmetadata_col) == 7, "tds_colmetadata_col has wrong size");

    using msg_handler = std::function<void(std::string_view server, std::string_view message, std::string_view proc_name,
                                      int32_t msgno, int32_t line_number, int16_t state, uint8_t severity, bool error)>;
    using func_count_handler = std::function<void(uint64_t count, uint16_t curcmd)>;

    class value;
    class tds_impl;

    class col_info {
    public:
        col_info(sql_type type, int16_t max_length, uint8_t precision, uint8_t scale,
                std::u16string_view collation, bool nullable, unsigned int codepage,
                std::u16string_view clr_name) :
                type(type), max_length(max_length), precision(precision), scale(scale),
                collation(collation), nullable(nullable), codepage(codepage),
                clr_name(clr_name) {
        }

        sql_type type;
        int16_t max_length;
        uint8_t precision;
        uint8_t scale;
        std::u16string collation;
        bool nullable;
        unsigned int codepage;
        std::u16string clr_name;
    };

    static constexpr size_t utf8_to_utf16_len(std::string_view sv) noexcept {
        size_t ret = 0;

        while (!sv.empty()) {
            if ((uint8_t)sv[0] < 0x80) {
                ret++;
                sv = sv.substr(1);
            } else if (((uint8_t)sv[0] & 0xe0) == 0xc0 && (uint8_t)sv.length() >= 2 && ((uint8_t)sv[1] & 0xc0) == 0x80) {
                ret++;
                sv = sv.substr(2);
            } else if (((uint8_t)sv[0] & 0xf0) == 0xe0 && (uint8_t)sv.length() >= 3 && ((uint8_t)sv[1] & 0xc0) == 0x80 && ((uint8_t)sv[2] & 0xc0) == 0x80) {
                ret++;
                sv = sv.substr(3);
            } else if (((uint8_t)sv[0] & 0xf8) == 0xf0 && (uint8_t)sv.length() >= 4 && ((uint8_t)sv[1] & 0xc0) == 0x80 && ((uint8_t)sv[2] & 0xc0) == 0x80 && ((uint8_t)sv[3] & 0xc0) == 0x80) {
                char32_t cp = (char32_t)(((uint8_t)sv[0] & 0x7) << 18) | (char32_t)(((uint8_t)sv[1] & 0x3f) << 12) | (char32_t)(((uint8_t)sv[2] & 0x3f) << 6) | (char32_t)((uint8_t)sv[3] & 0x3f);

                if (cp > 0x10ffff) {
                    ret++;
                    sv = sv.substr(4);
                    continue;
                }

                ret += 2;
                sv = sv.substr(4);
            } else {
                ret++;
                sv = sv.substr(1);
            }
        }

        return ret;
    }

    static constexpr size_t utf8_to_utf16_len(std::u8string_view sv) noexcept {
        return utf8_to_utf16_len(std::string_view(std::bit_cast<char*>(sv.data()), sv.length()));
    }

    template<typename T>
    requires (std::ranges::output_range<T, char16_t> && std::is_same_v<std::ranges::range_value_t<T>, char16_t>) ||
        (sizeof(wchar_t) == 2 && std::ranges::output_range<T, wchar_t> && std::is_same_v<std::ranges::range_value_t<T>, wchar_t>)
    static constexpr void utf8_to_utf16_range(std::string_view sv, T& t) noexcept {
        auto ptr = t.begin();

        if (ptr == t.end())
            return;

        while (!sv.empty()) {
            if ((uint8_t)sv[0] < 0x80) {
                *ptr = (uint8_t)sv[0];
                ptr++;

                if (ptr == t.end())
                    return;

                sv = sv.substr(1);
            } else if (((uint8_t)sv[0] & 0xe0) == 0xc0 && (uint8_t)sv.length() >= 2 && ((uint8_t)sv[1] & 0xc0) == 0x80) {
                char16_t cp = (char16_t)(((uint8_t)sv[0] & 0x1f) << 6) | (char16_t)((uint8_t)sv[1] & 0x3f);

                *ptr = cp;
                ptr++;

                if (ptr == t.end())
                    return;

                sv = sv.substr(2);
            } else if (((uint8_t)sv[0] & 0xf0) == 0xe0 && (uint8_t)sv.length() >= 3 && ((uint8_t)sv[1] & 0xc0) == 0x80 && ((uint8_t)sv[2] & 0xc0) == 0x80) {
                char16_t cp = (char16_t)(((uint8_t)sv[0] & 0xf) << 12) | (char16_t)(((uint8_t)sv[1] & 0x3f) << 6) | (char16_t)((uint8_t)sv[2] & 0x3f);

                if (cp >= 0xd800 && cp <= 0xdfff) {
                    *ptr = 0xfffd;
                    ptr++;

                    if (ptr == t.end())
                        return;

                    sv = sv.substr(3);
                    continue;
                }

                *ptr = cp;
                ptr++;

                if (ptr == t.end())
                    return;

                sv = sv.substr(3);
            } else if (((uint8_t)sv[0] & 0xf8) == 0xf0 && (uint8_t)sv.length() >= 4 && ((uint8_t)sv[1] & 0xc0) == 0x80 && ((uint8_t)sv[2] & 0xc0) == 0x80 && ((uint8_t)sv[3] & 0xc0) == 0x80) {
                char32_t cp = (char32_t)(((uint8_t)sv[0] & 0x7) << 18) | (char32_t)(((uint8_t)sv[1] & 0x3f) << 12) | (char32_t)(((uint8_t)sv[2] & 0x3f) << 6) | (char32_t)((uint8_t)sv[3] & 0x3f);

                if (cp > 0x10ffff) {
                    *ptr = 0xfffd;
                    ptr++;

                    if (ptr == t.end())
                        return;

                    sv = sv.substr(4);
                    continue;
                }

                cp -= 0x10000;

                *ptr = (char16_t)(0xd800 | (cp >> 10));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (char16_t)(0xdc00 | (cp & 0x3ff));
                ptr++;

                if (ptr == t.end())
                    return;

                sv = sv.substr(4);
            } else {
                *ptr = 0xfffd;
                ptr++;

                if (ptr == t.end())
                    return;

                sv = sv.substr(1);
            }
        }
    }

    template<typename T>
    requires (std::ranges::output_range<T, char16_t> && std::is_same_v<std::ranges::range_value_t<T>, char16_t>) ||
        (sizeof(wchar_t) == 2 && std::ranges::output_range<T, wchar_t> && std::is_same_v<std::ranges::range_value_t<T>, wchar_t>)
    static constexpr void utf8_to_utf16_range(std::u8string_view sv, T& t) noexcept {
        utf8_to_utf16_range(std::string_view((char*)sv.data(), sv.length()), t);
    }

    static constexpr std::u16string utf8_to_utf16(std::string_view sv) {
        if (sv.empty())
            return u"";

        std::u16string ret(utf8_to_utf16_len(sv), 0);

        utf8_to_utf16_range(sv, ret);

        return ret;
    }

    static constexpr std::u16string utf8_to_utf16(std::u8string_view sv) {
        if (sv.empty())
            return u"";

        std::u16string ret(utf8_to_utf16_len(sv), 0);

        utf8_to_utf16_range(sv, ret);

        return ret;
    }

    template<typename T>
    requires (std::is_same_v<T, char16_t> || (sizeof(wchar_t) == 2 && std::is_same_v<T, wchar_t>))
    static constexpr size_t utf16_to_utf8_len(std::basic_string_view<T> sv) noexcept {
        size_t ret = 0;

        while (!sv.empty()) {
            if (sv[0] < 0x80)
                ret++;
            else if (sv[0] < 0x800)
                ret += 2;
            else if (sv[0] < 0xd800)
                ret += 3;
            else if (sv[0] < 0xdc00) {
                if (sv.length() < 2 || (sv[1] & 0xdc00) != 0xdc00) {
                    ret += 3;
                    sv = sv.substr(1);
                    continue;
                }

                ret += 4;
                sv = sv.substr(1);
            } else
                ret += 3;

            sv = sv.substr(1);
        }

        return ret;
    }

    template<typename T, size_t N>
    requires (std::is_same_v<T, char16_t> || (sizeof(wchar_t) == 2 && std::is_same_v<T, wchar_t>))
    static constexpr size_t utf16_to_utf8_len(const T (&str)[N]) noexcept {
        return utf16_to_utf8_len(std::basic_string_view<T>{str, N - 1});
    }

    template<typename T, typename U>
    requires (std::is_same_v<T, char16_t> || (sizeof(wchar_t) == 2 && std::is_same_v<T, wchar_t>)) &&
             ((std::ranges::output_range<U, char> && std::is_same_v<std::ranges::range_value_t<U>, char>) ||
             (std::ranges::output_range<U, char8_t> && std::is_same_v<std::ranges::range_value_t<U>, char8_t>))
    static constexpr void utf16_to_utf8_range(std::basic_string_view<T> sv, U& t) noexcept {
        auto ptr = t.begin();

        if (ptr == t.end())
            return;

        while (!sv.empty()) {
            if (sv[0] < 0x80) {
                *ptr = (std::ranges::range_value_t<U>)(uint8_t)sv[0];
                ptr++;

                if (ptr == t.end())
                    return;
            } else if (sv[0] < 0x800) {
                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0xc0 | (sv[0] >> 6));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | (sv[0] & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;
            } else if (sv[0] < 0xd800) {
                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0xe0 | (sv[0] >> 12));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | ((sv[0] >> 6) & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | (sv[0] & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;
            } else if (sv[0] < 0xdc00) {
                if (sv.length() < 2 || (sv[1] & 0xdc00) != 0xdc00) {
                    *ptr = (std::ranges::range_value_t<U>)(uint8_t)0xef;
                    ptr++;

                    if (ptr == t.end())
                        return;

                    *ptr = (std::ranges::range_value_t<U>)(uint8_t)0xbf;
                    ptr++;

                    if (ptr == t.end())
                        return;

                    *ptr = (std::ranges::range_value_t<U>)(uint8_t)0xbd;
                    ptr++;

                    if (ptr == t.end())
                        return;

                    sv = sv.substr(1);
                    continue;
                }

                auto cp = (char32_t)(0x10000 | ((sv[0] & ~0xd800) << 10) | (sv[1] & ~0xdc00));

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0xf0 | (cp >> 18));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | ((cp >> 12) & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | ((cp >> 6) & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | (cp & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;

                sv = sv.substr(1);
            } else if (sv[0] < 0xe000) {
                *ptr = (std::ranges::range_value_t<U>)(uint8_t)0xef;
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)0xbf;
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)0xbd;
                ptr++;

                if (ptr == t.end())
                    return;
            } else {
                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0xe0 | (sv[0] >> 12));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | ((sv[0] >> 6) & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;

                *ptr = (std::ranges::range_value_t<U>)(uint8_t)(0x80 | (sv[0] & 0x3f));
                ptr++;

                if (ptr == t.end())
                    return;
            }

            sv = sv.substr(1);
        }
    }

    static constexpr std::string utf16_to_utf8(std::u16string_view sv) {
        if (sv.empty())
            return "";

        std::string ret(utf16_to_utf8_len(sv), 0);

        utf16_to_utf8_range(sv, ret);

        return ret;
    }

#if defined(_WIN32) || __WCHAR_WIDTH__ == 16
    static constexpr std::string utf16_to_utf8(std::wstring_view sv) {
        if (sv.empty())
            return "";

        std::string ret(utf16_to_utf8_len(sv), 0);

        utf16_to_utf8_range(sv, ret);

        return ret;
    }
#endif

    std::string TDSCPP utf16_to_cp(std::u16string_view s, unsigned int codepage);
    std::u16string TDSCPP cp_to_utf16(std::string_view s, unsigned int codepage);

    template<typename T>
    requires std::ranges::input_range<T> && std::ranges::contiguous_range<T>
    static constexpr std::span<std::byte> to_bytes(const T& t) {
        return std::span<std::byte>{(std::byte*)std::ranges::cdata(t), std::ranges::size(t) * sizeof(std::ranges::range_value_t<T>)};
    }

#pragma pack(push,1)

    struct TDSCPP collation {
        collation() = default;
        collation(std::string_view s);
        std::string to_string() const;

        uint32_t lcid : 20;
        uint32_t ignore_case : 1;
        uint32_t ignore_accent : 1;
        uint32_t ignore_kana : 1;
        uint32_t ignore_width : 1;
        uint32_t binary : 1;
        uint32_t binary2 : 1;
        uint32_t utf8 : 1;
        uint32_t reserved : 1;
        uint32_t version : 4;
        uint8_t sort_id;
    };

    static_assert(sizeof(collation) == 5, "tds::collation has wrong size");

#pragma pack(pop)

    size_t TDSCPP bcp_colmetadata_size(const col_info& col);
    void TDSCPP bcp_colmetadata_data(uint8_t*& ptr, const col_info& col, std::u16string_view name);

    template<typename T>
    concept list_of_values = std::ranges::input_range<T> && std::is_convertible_v<std::ranges::range_value_t<T>, value>;

    template<typename T>
    concept list_of_list_of_values = std::ranges::input_range<T> && list_of_values<std::ranges::range_value_t<T>>;

    template<typename T>
    concept is_string = std::is_convertible_v<T, std::string_view>;

    template<typename T>
    concept is_u16string = std::is_convertible_v<T, std::u16string_view>;

    template<typename T>
    concept is_u8string = std::is_convertible_v<T, std::u8string_view>;

    template<typename T>
    concept string_or_u16string = is_string<T> || is_u16string<T>;

    template<typename T>
    concept list_of_u16string = std::ranges::input_range<T> && is_u16string<std::ranges::range_value_t<T>>;

    template<typename T>
    concept list_of_string = std::ranges::input_range<T> && is_string<std::ranges::range_value_t<T>>;

    template<typename T>
    concept list_of_string_or_u16string = list_of_string<T> || list_of_u16string<T>;

    template<typename T>
    concept is_optional = std::is_convertible_v<std::nullopt_t, T>;

    class tds;
    class session;

    template<typename T>
    concept tds_or_session = std::is_same_v<T, tds> || std::is_same_v<T, session>;

    enum class encryption_type : uint8_t {
        ENCRYPT_OFF,
        ENCRYPT_ON,
        ENCRYPT_NOT_SUP,
        ENCRYPT_REQ
    };

    struct options {
        options(std::string_view server, std::string_view user = "", std::string_view password = "",
                std::string_view app_name = "tdscpp", std::string_view db = "",
                const msg_handler& message_handler = nullptr, const func_count_handler& count_handler = nullptr,
                uint16_t port = 1433, encryption_type encrypt = encryption_type::ENCRYPT_OFF,
                bool check_certificate = false, unsigned int codepage = 0, bool mars = false,
                unsigned int rate_limit = 0, bool read_only_intent = false) :
                server(server), user(user), password(password), app_name(app_name), db(db),
                message_handler(message_handler), count_handler(count_handler), port(port),
                encrypt(encrypt), check_certificate(check_certificate), codepage(codepage),
                mars(mars), rate_limit(rate_limit), read_only_intent(read_only_intent) {
        }

        std::string server;
        std::string user;
        std::string password;
        std::string app_name;
        std::string db;
        msg_handler message_handler;
        func_count_handler count_handler;
        uint16_t port;
        encryption_type encrypt;
        bool check_certificate;
        unsigned int codepage;
        bool mars;
        unsigned int rate_limit;
        bool read_only_intent;
    };

    template<typename T, size_t arg_count>
    requires (std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t>)
    struct checker {
        consteval checker(const T* q) : sv(q) {
            if (sv.empty())
                throw;

            bool in_quotes = false;
            unsigned int brackets = 0, qm = 0;

            for (auto c : sv) {
                if (c == '\'')
                    in_quotes = !in_quotes;
                else if (!in_quotes) {
                    if (c == '(')
                        brackets++;
                    else if (c == ')') {
                        if (brackets == 0)
                            throw;

                        brackets--;
                    } else if (c == '?')
                        qm++;
                }
            }

            if (brackets != 0)
                throw;

            if (in_quotes)
                throw;

            if (qm != arg_count)
                throw;
        }

        std::basic_string_view<T> sv;
    };

    template<typename T>
    requires (std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t>)
    struct no_check {
        no_check(const T* q) : sv(q) { }
        no_check(std::basic_string_view<T> q) : sv(q) { }
        no_check(const std::basic_string<T>& q) : sv(q) { }

        std::basic_string_view<T> sv;
    };

    class TDSCPP tds {
    public:
        tds(const options& opts);

        template<typename... Args>
        requires std::is_constructible_v<options, Args...>
        tds(Args&&... args) : tds((const options&)options(args...)) { }

        tds(const tds&) = delete;
        tds& operator=(const tds&) = delete;

        tds(tds&& that) noexcept;

        ~tds();

        void run(std::type_identity_t<checker<char, 0>> s);
        void run(std::type_identity_t<checker<char16_t, 0>> s);
        void run(std::type_identity_t<checker<char8_t, 0>> s);

        template<typename T>
        void run(no_check<T> s);

        template<typename... Args>
        void run(std::type_identity_t<checker<char, sizeof...(Args)>> s, Args&&... args);

        template<typename... Args>
        void run(std::type_identity_t<checker<char16_t, sizeof...(Args)>> s, Args&&... args);

        template<typename... Args>
        void run(std::type_identity_t<checker<char8_t, sizeof...(Args)>> s, Args&&... args);

        template<typename T, typename... Args>
        void run(no_check<T> s, Args&&... args);

        template<typename... Args>
        void run_rpc(const string_or_u16string auto& rpc_name, Args&&... args);

        void run_rpc(const string_or_u16string auto& rpc_name);

        template<string_or_u16string T = std::u16string_view>
        void bcp(const string_or_u16string auto& table, const list_of_u16string auto& np,
                 const list_of_list_of_values auto& vp, const T& db = u"") {
            std::vector<col_info> cols;

            if constexpr (is_u16string<decltype(table)> && is_u16string<decltype(db)>)
                cols = bcp_start(*this, table, np, db);
            else if constexpr (is_u16string<decltype(table)>)
                cols = bcp_start(*this, table, np, utf8_to_utf16(db));
            else if constexpr (is_u16string<decltype(db)>)
                cols = bcp_start(*this, utf8_to_utf16(table), np, db);
            else
                cols = bcp_start(*this, utf8_to_utf16(table), np, utf8_to_utf16(db));

            // send COLMETADATA for rows
            auto buf = bcp_colmetadata(np, cols);

            for (const auto& v : vp) {
                auto buf2 = bcp_row(v, np, cols);

                auto oldlen = buf.size();
                buf.resize(oldlen + buf2.size());
                memcpy(&buf[oldlen], buf2.data(), buf2.size());
            }

            bcp_sendmsg(buf);
        }

        template<string_or_u16string T = std::u16string_view>
        void bcp(const string_or_u16string auto& table, const list_of_string auto& np,
                 const list_of_list_of_values auto& vp, const T& db = u"") {
            std::vector<std::u16string> np2;

            for (const auto& s : np) {
                np2.emplace_back(utf8_to_utf16(s));
            }

            bcp(table, np2, vp, db);
        }

        uint16_t spid() const;
        std::u16string db_name() const;
        collation current_collation() const;

        std::unique_ptr<tds_impl> impl;
        unsigned int codepage;

    private:
        void bcp_sendmsg(std::span<const uint8_t> msg);
    };

    using time_t = std::chrono::duration<int64_t, std::ratio<1, 10000000>>;

    class TDSCPP WARN_UNUSED datetime {
    public:
        constexpr datetime() = default;

        constexpr datetime(std::chrono::year year, std::chrono::month month, std::chrono::day day, uint8_t hour, uint8_t minute, uint8_t second) :
            d(year, month, day) {
            auto secs = std::chrono::seconds((hour * 3600) + (minute * 60) + second);

            t = std::chrono::duration_cast<time_t>(secs);
        }

        constexpr datetime(std::chrono::year year, std::chrono::month month, std::chrono::day day, time_t t) :
            d(year, month, day), t(t) { }

        constexpr datetime(const std::chrono::year_month_day& d, time_t t) : d(d), t(t) { }

        template<typename T, typename U>
        datetime(const std::chrono::year_month_day& d, std::chrono::duration<T, U> t) : d(d), t{std::chrono::duration_cast<time_t>(t)} { }

        template<typename T>
        constexpr datetime(const std::chrono::time_point<T>& chr) {
            d = std::chrono::floor<std::chrono::days>(chr);
            t = std::chrono::floor<time_t>(chr - std::chrono::floor<std::chrono::days>(chr));
        }

        constexpr operator std::chrono::time_point<std::chrono::system_clock>() const {
            return std::chrono::sys_days{d} + t;
        }

        constexpr std::strong_ordering operator<=>(const datetime& dt) const {
            auto r = d <=> dt.d;

            if (r != std::strong_ordering::equivalent)
                return r;

            return t <=> dt.t;
        }

        constexpr bool operator==(const datetime& dt) const {
            return (*this <=> dt) == std::strong_ordering::equivalent;
        }

        static datetime now();

        std::chrono::year_month_day d;
        time_t t;
    };

    class TDSCPP WARN_UNUSED datetimeoffset : public datetime {
    public:
        constexpr datetimeoffset() = default;
        constexpr datetimeoffset(std::chrono::year year, std::chrono::month month, std::chrono::day day, uint8_t hour, uint8_t minute, uint8_t second, int16_t offset) :
            datetime(year, month, day, hour, minute, second), offset(offset) {
            this->t -= std::chrono::minutes{offset};
            normalize();
        }

        constexpr datetimeoffset(const std::chrono::year_month_day& d, time_t t, int16_t offset) : datetime(d, t), offset(offset) {
            this->t -= std::chrono::minutes{offset};
            normalize();
        }

        constexpr datetimeoffset(std::chrono::year year, std::chrono::month month, std::chrono::day day, time_t t, int16_t offset) :
            datetime(year, month, day, t), offset(offset) {
            this->t -= std::chrono::minutes{offset};
            normalize();
        }

        constexpr datetimeoffset(const std::chrono::time_point<std::chrono::system_clock>& t, int16_t offset) :
            datetime(t), offset(offset) {
            this->t -= std::chrono::minutes{offset};
            normalize();
        }

        template<typename T, typename U>
        constexpr datetimeoffset(const std::chrono::year_month_day& d2, std::chrono::duration<T, U> t2, int16_t offset) : offset(offset) {
            d = d2;
            t = std::chrono::duration_cast<time_t>(t2);
            t -= std::chrono::minutes{offset};
            normalize();
        }

        constexpr operator std::chrono::time_point<std::chrono::system_clock>() const {
            return std::chrono::sys_days{d} + t;
        }

        constexpr std::weak_ordering operator<=>(const datetime& dt) const {
            auto r = d <=> dt.d;

            // comparisons ignore the timezone - this is what MSSQL does

            if (r != std::weak_ordering::equivalent)
                return r;

            return t <=> dt.t;
        }

        constexpr std::weak_ordering operator<=>(const datetimeoffset& dto) const {
            auto r = d <=> dto.d;

            // comparisons ignore the timezone - this is what MSSQL does

            if (r != std::weak_ordering::equivalent)
                return r;

            return t <=> dto.t;
        }

        constexpr bool operator==(const datetime& dt) const {
            return (*this <=> dt) == std::weak_ordering::equivalent;
        }

        static datetimeoffset now();

        int16_t offset;

    private:
        constexpr void normalize() {
            if (t < time_t::zero()) {
                d = std::chrono::year_month_day{(std::chrono::sys_days)d - std::chrono::days{1}};
                t += std::chrono::days{1};
            } else if (t >= std::chrono::days{1}) {
                d = std::chrono::year_month_day{(std::chrono::sys_days)d + std::chrono::days{1}};
                t -= std::chrono::days{1};
            }
        }
    };

    template<typename T>
    concept byte_list = requires(T t) {
        { std::span<const std::byte>{t} };
    };

    template<unsigned N>
    requires (N <= 38)
    class numeric;

#if 0
    template<typename T, typename A = std::allocator<T>>
    class default_init_allocator : public A {
    public:
        typedef std::allocator_traits<A> a_t;

        template<typename U>
        struct rebind {
            using other = default_init_allocator<U, typename a_t::template rebind_alloc<U>>;
        };

        using A::A;

        template<typename U>
        void construct(U* ptr) noexcept(std::is_nothrow_default_constructible<U>::value) {
            ::new(static_cast<void*>(ptr)) U;
        }

        template<typename U, typename...Args>
        void construct(U* ptr, Args&&... args) {
            a_t::construct(static_cast<A&>(*this), ptr, std::forward<Args>(args)...);
        }
    };
#endif

    using value_data_t = std::vector<uint8_t>;

    class TDSCPP WARN_UNUSED value {
    public:
        // make sure pointers don't get interpreted as bools
        template<typename T>
        value(T*) = delete;

        value(wchar_t) = delete;

        constexpr value() {
            type = (sql_type)0;
        }

        constexpr value(std::nullptr_t) {
            type = sql_type::SQL_NULL;
            is_null = true;
        }

        template<typename T>
        requires std::is_same_v<T, uint8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, float> || std::is_same_v<T, double>
        constexpr value(T i) {
            if constexpr (std::is_floating_point_v<T>)
                type = sql_type::FLTN;
            else
                type = sql_type::INTN;

            auto arr = std::bit_cast<std::array<uint8_t, sizeof(T)>>(i);

            val.reserve(sizeof(T));
            std::copy(arr.begin(), arr.end(), std::back_inserter(val));
        }

        template<typename T>
        requires std::is_same_v<T, uint8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, float> || std::is_same_v<T, double>
        constexpr value(const std::optional<T>& i) {
            if constexpr (std::is_floating_point_v<T>)
                type = sql_type::FLTN;
            else
                type = sql_type::INTN;

            if (!i.has_value()) {
                is_null = true;
                val.resize(sizeof(T));
            } else {
                auto arr = std::bit_cast<std::array<uint8_t, sizeof(T)>>(i.value());

                val.reserve(sizeof(T));
                std::copy(arr.begin(), arr.end(), std::back_inserter(val));
            }
        }

        constexpr value(uint32_t i) : value((int64_t)i) {
        }

        constexpr value(const std::optional<uint32_t>& i) : value(i.has_value() ? std::optional<int64_t>(i.value()) : std::optional<int64_t>(std::nullopt)) {
        }

        template<typename... Args>
        requires std::is_constructible_v<collation, Args...>
        constexpr value(const std::u16string& sv, Args&&... args) : value(std::u16string_view(sv), args...) {
        }

        template<typename... Args>
        requires std::is_constructible_v<collation, Args...>
        constexpr value(const char16_t* sv, Args&&... args) : value(std::u16string_view(sv), args...) {
        }

        template<typename T>
        requires (std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t>)
        constexpr value(const std::optional<std::basic_string_view<T>>& sv) {
            if constexpr (std::is_same_v<T, char16_t>)
                type = sql_type::NVARCHAR;
            else
                type = sql_type::VARCHAR;

            if (!sv.has_value())
                is_null = true;
            else {
                val.reserve(sv.value().length() * sizeof(T));

                if constexpr (std::is_same_v<T, char16_t>) {
                    for (auto c : sv.value()) {
                        val.push_back((uint8_t)(c & 0xff));
                        val.push_back((uint8_t)(c >> 8));
                    }
                } else
                    std::copy(sv.value().begin(), sv.value().end(), std::back_inserter(val));
            }

            // Latin1_General_CI_AS

            coll.lcid = 1033;
            coll.ignore_case = 1;
            coll.ignore_accent = 0;
            coll.ignore_kana = 1;
            coll.ignore_width = 1;
            coll.binary = 0;
            coll.binary2 = 0;
            coll.sort_id = 0;

            if constexpr (std::is_same_v<T, char8_t>) {
                coll.version = 2;
                coll.utf8 = 1;
            } else {
                coll.version = 0;
                coll.utf8 = 0;
            }
        }

        template<typename T, typename... Args>
        requires ((std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t>) && std::is_constructible_v<collation, Args...>)
        constexpr value(const std::optional<std::basic_string_view<T>>& sv, Args&&... args) : coll(args...) {
            if constexpr (std::is_same_v<T, char16_t>)
                type = sql_type::NVARCHAR;
            else
                type = sql_type::VARCHAR;

            if constexpr (std::is_same_v<T, char8_t>)
                coll.utf8 = true; // FIXME - rather than changing collation, should be changing string into codepage if not UTF-8?

            if (!sv.has_value())
                is_null = true;
            else {
                val.resize(sv.value().length() * sizeof(T));
                memcpy(val.data(), sv.value().data(), val.size());
            }
        }

        template<typename T, typename... Args>
        requires ((std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t>) && std::is_constructible_v<collation, Args...>)
        constexpr value(const std::optional<std::basic_string<T>>& s, Args&&... args) : coll(args...) {
            if constexpr (std::is_same_v<T, char16_t>)
                type = sql_type::NVARCHAR;
            else
                type = sql_type::VARCHAR;

            if constexpr (std::is_same_v<T, char8_t>)
                coll.utf8 = true; // FIXME - rather than changing collation, should be changing string into codepage if not UTF-8?

            if (!s.has_value())
                is_null = true;
            else {
                val.reserve(s.value().length() * sizeof(T));

                if constexpr (std::is_same_v<T, char16_t>) {
                    for (auto c : s.value()) {
                        val.push_back((uint8_t)(c & 0xff));
                        val.push_back((uint8_t)(c >> 8));
                    }
                } else
                    std::copy(s.value().begin(), s.value().end(), std::back_inserter(val));
            }
        }

        template<typename T>
        requires (std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t>)
        constexpr value(std::basic_string_view<T> sv) {
            val.reserve(sv.length() * sizeof(T));

            if constexpr (std::is_same_v<T, char16_t>) {
                type = sql_type::NVARCHAR;

                for (auto c : sv) {
                    val.push_back((uint8_t)(c & 0xff));
                    val.push_back((uint8_t)(c >> 8));
                }
            } else {
                type = sql_type::VARCHAR;
                std::copy(sv.begin(), sv.end(), std::back_inserter(val));
            }

            // Latin1_General_CI_AS

            coll.lcid = 1033;
            coll.ignore_case = 1;
            coll.ignore_accent = 0;
            coll.ignore_kana = 1;
            coll.ignore_width = 1;
            coll.binary = 0;
            coll.binary2 = 0;
            coll.sort_id = 0;

            if constexpr (std::is_same_v<T, char8_t>) {
                coll.version = 2;
                coll.utf8 = 1;
            } else {
                coll.version = 0;
                coll.utf8 = 0;
            }
        }

        template<typename T, typename... Args>
        requires ((std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t>) && std::is_constructible_v<collation, Args...>)
        value(std::basic_string_view<T> sv, Args&&... args) : coll(args...) {
            if constexpr (std::is_same_v<T, char16_t>)
                type = sql_type::NVARCHAR;
            else
                type = sql_type::VARCHAR;

            if constexpr (std::is_same_v<T, char8_t>)
                coll.utf8 = true; // FIXME - rather than changing collation, should be changing string into codepage if not UTF-8?

            val.resize(sv.length() * sizeof(T));
            memcpy(val.data(), sv.data(), val.size());
        }

        template<typename... Args>
        requires std::is_constructible_v<collation, Args...>
        constexpr value(const std::string& sv, Args&&... args) : value(std::string_view(sv), args...) {
        }

        template<typename... Args>
        requires std::is_constructible_v<collation, Args...>
        constexpr value(const char* sv, Args&&... args) : value(std::string_view(sv), args...) {
        }

        template<typename... Args>
        requires std::is_constructible_v<collation, Args...>
        constexpr value(const std::u8string& sv, Args&&... args) : value(std::u8string_view(sv), args...) {
        }

        template<typename... Args>
        requires std::is_constructible_v<collation, Args...>
        constexpr value(const char8_t* sv, Args&&... args) : value(std::u8string_view(sv), args...) {
        }

        value(const std::chrono::year_month_day& d);
        value(const std::optional<std::chrono::year_month_day>& d);
        value(time_t t);
        value(const std::optional<time_t>& t);
        value(const datetime& dt);
        value(const std::optional<datetime>& t);
        value(const datetimeoffset& dt);
        value(const std::optional<datetimeoffset>& t);

        constexpr value(std::span<const std::byte> bin) {
            type = sql_type::VARBINARY;
            val.reserve(bin.size());

            for (auto c : bin) {
                val.push_back((uint8_t)c);
            }
        }

        template<typename T> requires byte_list<T>
        constexpr value(const std::optional<T>& bin) {
            type = sql_type::VARBINARY;

            if (!bin.has_value())
                is_null = true;
            else {
                const auto& s = std::span{bin.value()};
                val.reserve(s.size());

                for (auto c : s) {
                    val.push_back((uint8_t)c);
                }
            }
        }

        constexpr value(bool b) {
            type = sql_type::BITN;
            val.resize(sizeof(uint8_t));
            val[0] = b ? 1 : 0;
        }

        constexpr value(const std::optional<bool>& b) {
            type = sql_type::BITN;
            val.resize(sizeof(uint8_t));

            if (b.has_value())
                val[0] = b ? 1 : 0;
            else
                is_null = true;
        }

        value(const std::chrono::time_point<std::chrono::system_clock>& chr) : value((datetime)chr) { }

        template<typename T, typename U>
        value(std::chrono::duration<T, U> t) : value(std::chrono::duration_cast<time_t>(t)) { }

        template<unsigned N>
        value(const numeric<N>& n) noexcept {
            type = sql_type::NUMERIC;
            precision = 38;
            scale = N;
            val.resize(17);
            val[0] = n.neg ? 0 : 1;
            *(uint64_t*)&val[1] = n.low_part;
            *(uint64_t*)&val[1 + sizeof(uint64_t)] = n.high_part;
        }

        explicit operator std::string() const;
        explicit operator std::u8string() const;
        explicit operator std::u16string() const;
        explicit operator int64_t() const;
        explicit operator double() const;
        explicit operator std::chrono::year_month_day() const;
        explicit operator datetime() const;
        explicit operator datetimeoffset() const;

        explicit operator time_t() const;

        template<typename T, typename U>
        explicit operator std::chrono::duration<T, U>() const {
            return std::chrono::duration_cast<std::chrono::duration<T, U>>((time_t)*this);
        }

        template<typename T>
        requires std::is_integral_v<T>
        explicit operator T() const {
            return static_cast<T>(static_cast<int64_t>(*this));
        }

        template<typename T>
        requires std::is_floating_point_v<T>
        explicit operator T() const {
            return static_cast<T>(static_cast<double>(*this));
        }

        explicit operator std::chrono::time_point<std::chrono::system_clock>() const {
            return static_cast<std::chrono::time_point<std::chrono::system_clock>>(static_cast<datetime>(*this));
        }

        template<unsigned N>
        explicit operator numeric<N>() const {
            auto type2 = type;
            std::span d = val;

            if (is_null)
                return 0;

            if (type2 == sql_type::SQL_VARIANT) {
                type2 = (sql_type)d[0];
                d = d.subspan(1);
                auto propbytes = d[0];
                d = d.subspan(1 + propbytes);
            }

            switch (type2) {
                case sql_type::TINYINT:
                case sql_type::SMALLINT:
                case sql_type::INT:
                case sql_type::BIGINT:
                case sql_type::INTN:
                    return (int64_t)*this;

                case sql_type::NUMERIC:
                case sql_type::DECIMAL: {
                    numeric<N> n;

                    n.neg = d[0] == 0;

                    if (d.size() >= 9)
                        n.low_part = *(uint64_t*)&d[1];
                    else
                        n.low_part = *(uint32_t*)&d[1];

                    if (d.size() >= 17)
                        n.high_part = *(uint64_t*)&d[1 + sizeof(uint64_t)];
                    else if (d.size() >= 13)
                        n.high_part = *(uint32_t*)&d[1 + sizeof(uint64_t)];
                    else
                        n.high_part = 0;

                    if (N < scale) {
                        for (unsigned int i = N; i < scale; i++) {
                            n.ten_div();
                        }
                    } else if (N > scale) {
                        for (unsigned int i = scale; i < N; i++) {
                            n.ten_mult();
                        }
                    }

                    return n;
                }

                // FIXME - REAL / FLOAT

                default:
                    return (int64_t)*this; // FIXME - should be double when supported
            }
        }

        std::string collation_name() const;
        std::partial_ordering operator<=>(const value& v) const;

        bool operator==(const value& v) const {
            return (*this <=> v) == std::partial_ordering::equivalent;
        }

        std::string to_literal() const;

        enum sql_type type;
        value_data_t val;
        bool is_null = false;
        bool is_output = false;
        unsigned int max_length = 0;
        uint8_t precision;
        uint8_t scale;
        collation coll;
        std::u16string clr_name;
    };

    class TDSCPP column : public value {
    public:
        std::u16string name;
        bool nullable;
    };

    template<typename T>
    class output_param : public value {
    public:
        output_param() : value(std::optional<T>(std::nullopt)) {
        }
    };

    class smp_session;

    class TDSCPP session {
    public:
        session(tds& conn);
        ~session();

        void run(std::type_identity_t<checker<char, 0>> s);
        void run(std::type_identity_t<checker<char16_t, 0>> s);
        void run(std::type_identity_t<checker<char8_t, 0>> s);

        template<typename T>
        void run(no_check<T> s);

        tds& conn;
        std::unique_ptr<smp_session> impl;

        template<string_or_u16string T = std::u16string_view>
        void bcp(const string_or_u16string auto& table, const list_of_u16string auto& np,
                 const list_of_list_of_values auto& vp, const T& db = u"") {
            std::vector<col_info> cols;

            if constexpr (is_u16string<decltype(table)> && is_u16string<decltype(db)>)
                cols = bcp_start(*this, table, np, db);
            else if constexpr (is_u16string<decltype(table)>)
                cols = bcp_start(*this, table, np, utf8_to_utf16(db));
            else if constexpr (is_u16string<decltype(db)>)
                cols = bcp_start(*this, utf8_to_utf16(table), np, db);
            else
                cols = bcp_start(*this, utf8_to_utf16(table), np, utf8_to_utf16(db));

            // send COLMETADATA for rows
            auto buf = bcp_colmetadata(np, cols);

            for (const auto& v : vp) {
                auto buf2 = bcp_row(v, np, cols);

                auto oldlen = buf.size();
                buf.resize(oldlen + buf2.size());
                memcpy(&buf[oldlen], buf2.data(), buf2.size());
            }

            bcp_sendmsg(buf);
        }

        template<string_or_u16string T = std::u16string_view>
        void bcp(const string_or_u16string auto& table, const list_of_string auto& np,
                 const list_of_list_of_values auto& vp, const T& db = u"") {
            std::vector<std::u16string> np2;

            for (const auto& s : np) {
                np2.emplace_back(utf8_to_utf16(s));
            }

            bcp(table, np2, vp, db);
        }

    private:
        void bcp_sendmsg(std::span<const uint8_t> msg);
    };

    class TDSCPP rpc {
    public:
        ~rpc();

        template<typename... Args>
        rpc(tds& tds, std::u16string_view rpc_name, Args&&... args) : conn(tds) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_rpc(rpc_name);
        }

        rpc(tds& tds, std::u16string_view rpc_name) : conn(tds) {
            do_rpc(rpc_name);
        }

        template<typename... Args>
        rpc(tds& tds, std::string_view rpc_name, Args&&... args) : conn(tds) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_rpc(utf8_to_utf16(rpc_name));
        }

        rpc(tds& tds, std::string_view rpc_name) : conn(tds) {
            do_rpc(utf8_to_utf16(rpc_name));
        }

        template<typename... Args>
        rpc(session& sess, std::u16string_view rpc_name, Args&&... args) : conn(sess.conn) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_rpc(sess, rpc_name);
        }

        rpc(session& sess, std::u16string_view rpc_name) : conn(sess.conn) {
            do_rpc(sess, rpc_name);
        }

        template<typename... Args>
        rpc(session& sess, std::string_view rpc_name, Args&&... args) : conn(sess.conn) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_rpc(sess, utf8_to_utf16(rpc_name));
        }

        rpc(session& sess, std::string_view rpc_name) : conn(sess.conn) {
            do_rpc(sess, utf8_to_utf16(rpc_name));
        }

        uint16_t num_columns() const;

        const column& operator[](uint16_t i) const;
        column& operator[](uint16_t i);

        bool fetch_row();
        bool fetch_row_no_wait();

        int32_t return_status = 0;
        std::vector<column> cols;

    private:
        template<typename T, typename... Args>
        void add_param(T&& t, Args&&... args) {
            add_param(t);
            add_param(args...);
        }

        template<typename T>
        void add_param(T&& t) {
            params.emplace_back(t);
        }

        template<typename T>
        void add_param(output_param<T>& t) {
            params.emplace_back(static_cast<value>(t));
            params.back().is_output = true;

            output_params[(unsigned int)(params.size() - 1)] = static_cast<value*>(&t);
        }

        template<typename T> requires (std::ranges::input_range<T> && !byte_list<T> && !is_string<T> && !is_u16string<T> && !is_u8string<T>)
        void add_param(T&& v) {
            for (const auto& t : v) {
                params.emplace_back(t);
            }
        }

        template<typename T>
        void add_param(const std::optional<T>& v) {
            if (!v.has_value()) {
                params.emplace_back("");
                params.back().is_null = true;
            } else
                params.emplace_back(v.value());
        }

        void do_rpc(std::u16string_view name);
        void do_rpc(session& sess2, std::u16string_view name);
        void wait_for_packet();

        tds& conn;
        std::vector<value> params;
        std::map<unsigned int, value*> output_params;
        bool finished = false, received_attn = false;
        std::list<std::vector<std::pair<value_data_t, bool>>> rows;
        std::list<std::vector<uint8_t>> tokens;
        std::vector<uint8_t> buf;
        std::vector<column> buf_columns;
        std::u16string name;
        std::optional<std::reference_wrapper<smp_session>> sess;
    };

    class TDSCPP query {
    public:
        query(tds& tds, std::type_identity_t<checker<char, 0>> q) : conn(tds) {
            do_query(cp_to_utf16(q.sv, tds.codepage));
        }

        query(tds& tds, std::type_identity_t<checker<char8_t, 0>> q) : conn(tds) {
            do_query(utf8_to_utf16(q.sv));
        }

        query(tds& tds, std::type_identity_t<checker<char16_t, 0>> q) : conn(tds) {
            do_query(q.sv);
        }

        template<typename T>
        query(tds& tds, no_check<T> q) : conn(tds) {
            if constexpr (std::is_same_v<T, char>)
                do_query(cp_to_utf16(q.sv, tds.codepage));
            else if constexpr (std::is_same_v<T, char8_t>)
                do_query(utf8_to_utf16(q.sv));
            else
                do_query(q.sv);
        }

        template<typename... Args>
        query(tds& tds, std::type_identity_t<checker<char, sizeof...(Args)>> q, Args&&... args) : conn(tds) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_query(cp_to_utf16(q.sv, tds.codepage));
        }

        template<typename... Args>
        query(tds& tds, std::type_identity_t<checker<char8_t, sizeof...(Args)>> q, Args&&... args) : conn(tds) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_query(utf8_to_utf16(q.sv));
        }

        template<typename... Args>
        query(tds& tds, std::type_identity_t<checker<char16_t, sizeof...(Args)>> q, Args&&... args) : conn(tds) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_query(q.sv);
        }

        template<typename T, typename... Args>
        query(tds& tds, no_check<T> q, Args&&... args) : conn(tds) {
            params.reserve(sizeof...(args));

            add_param(args...);

            if constexpr (std::is_same_v<T, char>)
                do_query(cp_to_utf16(q.sv, tds.codepage));
            else if constexpr (std::is_same_v<T, char8_t>)
                do_query(utf8_to_utf16(q.sv));
            else
                do_query(q.sv);
        }

        query(session& sess, std::type_identity_t<checker<char, 0>> q) : conn(sess.conn) {
            do_query(sess, cp_to_utf16(q.sv, sess.conn.codepage));
        }

        query(session& sess, std::type_identity_t<checker<char8_t, 0>> q) : conn(sess.conn) {
            do_query(sess, utf8_to_utf16(q.sv));
        }

        query(session& sess, std::type_identity_t<checker<char16_t, 0>> q) : conn(sess.conn) {
            do_query(sess, q.sv);
        }

        template<typename T>
        query(session& sess, no_check<T> q) : conn(sess.conn) {
            if constexpr (std::is_same_v<T, char>)
                do_query(sess, cp_to_utf16(q.sv, sess.conn.codepage));
            else if constexpr (std::is_same_v<T, char8_t>)
                do_query(sess, utf8_to_utf16(q.sv));
            else
                do_query(sess, q.sv);
        }

        template<typename... Args>
        query(session& sess, std::type_identity_t<checker<char, sizeof...(Args)>> q, Args&&... args) : conn(sess.conn) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_query(sess, cp_to_utf16(q.sv, sess.conn.codepage));
        }

        template<typename... Args>
        query(session& sess, std::type_identity_t<checker<char8_t, sizeof...(Args)>> q, Args&&... args) : conn(sess.conn) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_query(sess, utf8_to_utf16(q.sv));
        }

        template<typename... Args>
        query(session& sess, std::type_identity_t<checker<char16_t, sizeof...(Args)>> q, Args&&... args) : conn(sess.conn) {
            params.reserve(sizeof...(args));

            add_param(args...);

            do_query(sess, q.sv);
        }

        template<typename T, typename... Args>
        query(session& sess, no_check<T> q, Args&&... args) : conn(sess.conn) {
            params.reserve(sizeof...(args));

            add_param(args...);

            if constexpr (std::is_same_v<T, char>)
                do_query(sess, cp_to_utf16(q.sv, sess.conn.codepage));
            else if constexpr (std::is_same_v<T, char8_t>)
                do_query(sess, utf8_to_utf16(q.sv));
            else
                do_query(sess, q.sv);
        }

        query(const query&) = delete;

        ~query();

        uint16_t num_columns() const;

        const column& operator[](uint16_t i) const;
        column& operator[](uint16_t i);

        bool fetch_row();
        bool fetch_row_no_wait();

    private:
        void do_query(std::u16string_view q);
        void do_query(session& sess, std::u16string_view q);

        template<typename T, typename... Args>
        void add_param(T&& t, Args&&... args) {
            add_param(t);
            add_param(args...);
        }

        template<typename T>
        void add_param(T&& t) {
            params.emplace_back(t);
        }

        template<typename T> requires (std::ranges::input_range<T> && !byte_list<T> && !is_string<T> && !is_u16string<T> && !is_u8string<T>)
        void add_param(T&& v) {
            for (const auto& t : v) {
                params.emplace_back(t);
            }
        }

        template<typename T> requires byte_list<T>
        void add_param(const T& bin) {
            params.emplace_back(bin);
        }

        template<typename T> requires byte_list<T>
        void add_param(const std::optional<T>& bin) {
            if (!bin.has_value()) {
                params.emplace_back("");
                params.back().is_null = true;
            } else
                params.emplace_back(bin.value());
        }

        template<typename T>
        void add_param(const std::optional<T>& v) {
            if (!v.has_value()) {
                params.emplace_back("");
                params.back().is_null = true;
            } else
                params.emplace_back(v.value());
        }

        tds& conn;
        std::vector<value> params;
        std::vector<column> cols;
        std::unique_ptr<rpc> r2;
        output_param<int32_t> handle;
        std::optional<std::reference_wrapper<session>> sess;
    };

    template<typename... Args>
    void tds::run(std::type_identity_t<checker<char, sizeof...(Args)>> s, Args&&... args) {
        query q(*this, no_check(s.sv), args...);

        while (q.fetch_row()) {
        }
    }

    template<typename... Args>
    void tds::run(std::type_identity_t<checker<char16_t, sizeof...(Args)>> s, Args&&... args) {
        query q(*this, no_check(s.sv), args...);

        while (q.fetch_row()) {
        }
    }

    template<typename... Args>
    void tds::run(std::type_identity_t<checker<char8_t, sizeof...(Args)>> s, Args&&... args) {
        query q(*this, no_check(s.sv), args...);

        while (q.fetch_row()) {
        }
    }

    template<typename T, typename... Args>
    void tds::run(no_check<T> s, Args&&... args) {
        query q(*this, s, args...);

        while (q.fetch_row()) {
        }
    }

    class batch_impl;

    class TDSCPP batch {
    public:
        batch(tds& conn, std::type_identity_t<checker<char, 0>> q) {
            do_batch(conn, cp_to_utf16(q.sv, conn.codepage));
        }

        batch(tds& conn, std::type_identity_t<checker<char8_t, 0>> q) {
            do_batch(conn, utf8_to_utf16(q.sv));
        }

        batch(tds& conn, std::type_identity_t<checker<char16_t, 0>> q) {
            do_batch(conn, q.sv);
        }

        template<typename T>
        batch(tds& conn, no_check<T> q) {
            if constexpr (std::is_same_v<T, char>)
                do_batch(conn, cp_to_utf16(q.sv, conn.codepage));
            else if constexpr (std::is_same_v<T, char8_t>)
                do_batch(conn, utf8_to_utf16(q.sv));
            else
                do_batch(conn, q.sv);
        }

        batch(session& sess, std::type_identity_t<checker<char, 0>> q) {
            do_batch(sess, cp_to_utf16(q.sv, sess.conn.codepage));
        }

        batch(session& sess, std::type_identity_t<checker<char8_t, 0>> q) {
            do_batch(sess, utf8_to_utf16(q.sv));
        }

        batch(session& sess, std::type_identity_t<checker<char16_t, 0>> q) {
            do_batch(sess, q.sv);
        }

        template<typename T>
        batch(session& sess, no_check<T> q) {
            if constexpr (std::is_same_v<T, char>)
                do_batch(sess, cp_to_utf16(q.sv, sess.conn.codepage));
            else if constexpr (std::is_same_v<T, char8_t>)
                do_batch(sess, utf8_to_utf16(q.sv));
            else
                do_batch(sess, q.sv);
        }

        ~batch();

        uint16_t num_columns() const;
        const column& operator[](uint16_t i) const;
        column& operator[](uint16_t i);
        bool fetch_row();

    private:
        void do_batch(tds& conn, std::u16string_view q);
        void do_batch(session& sess, std::u16string_view q);

        batch_impl* impl;
    };

    void __inline tds::run(std::type_identity_t<checker<char, 0>> s) {
        batch b(*this, no_check(s.sv));

        while (b.fetch_row()) {
        }
    }

    void __inline tds::run(std::type_identity_t<checker<char16_t, 0>> s) {
        batch b(*this, no_check(s.sv));

        while (b.fetch_row()) {
        }
    }

    void __inline tds::run(std::type_identity_t<checker<char8_t, 0>> s) {
        batch b(*this, no_check(s.sv));

        while (b.fetch_row()) {
        }
    }

    template<typename T>
    void tds::run(no_check<T> s) {
        batch b(*this, s);

        while (b.fetch_row()) {
        }
    }

    template<typename... Args>
    void tds::run_rpc(const string_or_u16string auto& rpc_name, Args&&... args) {
        rpc r(*this, rpc_name, args...);

        while (r.fetch_row()) {
        }
    }

    void tds::run_rpc(const string_or_u16string auto& rpc_name) {
        rpc r(*this, rpc_name);

        while (r.fetch_row()) {
        }
    }

    void __inline session::run(std::type_identity_t<checker<char, 0>> s) {
        batch b(*this, no_check(s.sv));

        while (b.fetch_row()) {
        }
    }

    void __inline session::run(std::type_identity_t<checker<char16_t, 0>> s) {
        batch b(*this, no_check(s.sv));

        while (b.fetch_row()) {
        }
    }

    void __inline session::run(std::type_identity_t<checker<char8_t, 0>> s) {
        batch b(*this, no_check(s.sv));

        while (b.fetch_row()) {
        }
    }

    template<typename T>
    void session::run(no_check<T> s) {
        batch b(*this, s);

        while (b.fetch_row()) {
        }
    }

    class TDSCPP trans {
    public:
        trans(tds& conn);
        ~trans();
        void commit();

    private:
        tds& conn;
        bool committed = false;
    };

    void TDSCPP to_json(nlohmann::json& j, const value& v);

    static void __inline to_json(nlohmann::json& j, const column& c) {
        to_json(j, static_cast<const value&>(c));
    }

    static std::string __inline escape(std::string_view sv) {
        std::string s{"["};

        s.reserve(sv.length() + 2);

        for (const auto& c : sv) {
                if (c == ']')
                    s += "]]";
                else
                    s += c;
        }

        s += "]";

        return s;
    }

    static std::u16string __inline escape(std::u16string_view sv) {
        std::u16string s{u"["};

        s.reserve(sv.length() + 2);

        for (const auto& c : sv) {
            if (c == u']')
                s += u"]]";
            else
                s += c;
        }

        s += u"]";

        return s;
    }

    template<typename T>
    struct object_name_parts {
        std::basic_string_view<T> server;
        std::basic_string_view<T> db;
        std::basic_string_view<T> schema;
        std::basic_string_view<T> name;
    };

    template<typename T>
    concept is_char_type = std::is_same_v<T, char> || std::is_same_v<T, char8_t> || std::is_same_v<T, char16_t> || std::is_same_v<T, wchar_t> || std::is_same_v<T, char32_t>;

    template<typename T>
    requires is_char_type<T>
    static constexpr object_name_parts<T> parse_object_name(std::basic_string_view<T> s) noexcept {
        bool escaped = false;
        size_t partstart = 0, partnum = 0;
        std::array<std::basic_string_view<T>, 4> parts;

        for (size_t i = 0; i < s.length(); i++) {
            if (!escaped && s[i] == '[')
                escaped = true;
            else if (escaped && s[i] == ']') {
                if (i + 1 < s.length() && s[i+1] == ']') {
                    i++;
                    continue;
                }

                escaped = false;
            } else if (!escaped && s[i] == '.') {
                if (partnum < parts.size()) {
                    parts[partnum] = std::basic_string_view<T>(&s[partstart], i - partstart);
                    partstart = i + 1;
                    partnum++;
                }
            }
        }

        if (partnum < parts.size()) {
            parts[partnum] = std::basic_string_view<T>(&s[partstart], s.length() - partstart);
            partnum++;
        }

        object_name_parts<T> onp;

        switch (partnum) {
            case 1:
                onp.name = parts[0];
                break;

            case 2:
                onp.schema = parts[0];
                onp.name = parts[1];
                break;

            case 3:
                onp.db = parts[0];
                onp.schema = parts[1];
                onp.name = parts[2];
                break;

            default:
                onp.server = parts[0];
                onp.db = parts[1];
                onp.schema = parts[2];
                onp.name = parts[3];
                break;
        }

        return onp;
    }

    template<typename T>
    requires is_char_type<T>
    static constexpr object_name_parts<T> parse_object_name(const T* s) noexcept {
        return parse_object_name(std::basic_string_view<T>{s});
    }

    template<typename T>
    requires is_char_type<T>
    static constexpr object_name_parts<T> parse_object_name(const std::basic_string<T>& s) noexcept {
        return parse_object_name(std::basic_string_view<T>{s});
    }

    uint16_t TDSCPP get_instance_port(const std::string& server, std::string_view instance);
    size_t TDSCPP bcp_row_size(const col_info& col, const value& vv);
    void TDSCPP bcp_row_data(uint8_t*& ptr, const col_info& col, const value& vv, std::u16string_view col_name);

    std::vector<uint8_t> bcp_row(const list_of_values auto& v, const list_of_u16string auto& np, const std::vector<col_info>& cols) {
        size_t bufsize = sizeof(uint8_t);

        auto it = v.begin();
        auto it2 = np.begin();
        unsigned int num_cols = 0;

        for (const auto& col : cols) {
            const auto& vv = *it;

            if (it == v.end()) {
                if constexpr (std::ranges::sized_range<decltype(v)>)
                    throw std::runtime_error("Trying to send " + std::to_string(v.size()) + " columns in a BCP row, expected " + std::to_string(cols.size()) + ".");
                else
                    throw std::runtime_error("Trying to send " + std::to_string(num_cols) + " columns in a BCP row, expected " + std::to_string(cols.size()) + ".");
            }

            if constexpr (std::is_same_v<std::ranges::range_value_t<decltype(v)>, value>) {
                if (vv.is_null && !col.nullable)
                    throw std::runtime_error("Cannot insert NULL into column " + utf16_to_utf8(*it2) + " marked NOT NULL.");
            } else if constexpr (is_optional<std::ranges::range_value_t<decltype(v)>>) {
                if (!vv.has_value() && !col.nullable)
                    throw std::runtime_error("Cannot insert NULL into column " + utf16_to_utf8(*it2) + " marked NOT NULL.");
            }

            bufsize += bcp_row_size(col, vv);
            it++;
            it2++;
            num_cols++;
        }

        std::vector<uint8_t> buf(bufsize);
        uint8_t* ptr = buf.data();

        *(token*)ptr = token::ROW;
        ptr++;

        it = v.begin();
        it2 = np.begin();

        for (const auto& col : cols) {
            const auto& vv = *it;

            bcp_row_data(ptr, col, vv, *it2);

            it++;
            it2++;
        }

        return buf;
    }

    std::vector<uint8_t> bcp_colmetadata(const list_of_u16string auto& np, const std::vector<col_info>& cols) {
        size_t bufsize = sizeof(uint8_t) + sizeof(uint16_t) + (cols.size() * sizeof(tds_colmetadata_col));

        for (const auto& col : cols) {
            bufsize += bcp_colmetadata_size(col) + sizeof(uint8_t);
        }

        for (const auto& n : np) {
            bufsize += std::u16string_view{n}.length() * sizeof(char16_t);
        }

        std::vector<uint8_t> buf(bufsize);
        auto ptr = (uint8_t*)buf.data();

        *(token*)ptr = token::COLMETADATA; ptr++;
        *(uint16_t*)ptr = (uint16_t)cols.size(); ptr += sizeof(uint16_t);

        auto it = np.begin();

        for (unsigned int i = 0; i < cols.size(); i++) {
            const auto& col = cols[i];

            bcp_colmetadata_data(ptr, col, *it);

            it++;
        }

        return buf;
    }

    std::map<std::u16string, col_info> get_col_info(tds_or_session auto& n, std::u16string_view table,
                                                    std::u16string_view db);

    std::u16string TDSCPP type_to_string(enum sql_type type, size_t length, uint8_t precision, uint8_t scale,
                                         std::u16string_view collation, std::u16string_view clr_name);

    std::vector<col_info> bcp_start(tds_or_session auto& n, std::u16string_view table, const list_of_u16string auto& np,
                                    std::u16string_view db) {
        if (np.empty())
            throw std::runtime_error("List of columns not supplied.");

        // FIXME - do we need to make sure no duplicates in np?

        std::vector<col_info> cols;

        {
            auto col_info = get_col_info(n, table, db);

            if constexpr (std::ranges::sized_range<decltype(np)>)
                cols.reserve(np.size());

            for (const auto& n : np) {
                if (col_info.count(n) == 0)
                    throw std::runtime_error("Column " + utf16_to_utf8(n) + " not found in table " + utf16_to_utf8(table) + ".");

                cols.emplace_back(col_info.at(n));
            }
        }

        {
            std::u16string q = u"INSERT BULK " + (!db.empty() ? (std::u16string(db) + u".") : u"") + std::u16string(table) + u"(";
            bool first = true;

            auto it = np.begin();

            for (const auto& col : cols) {
                if (!first)
                    q += u", ";

                q += escape(*it) + u" ";

                if (col.type == sql_type::UDT)
                    q += u"VARBINARY(MAX)";
                else
                    q += type_to_string(col.type, (size_t)col.max_length, col.precision, col.scale, col.collation, col.clr_name);

                first = false;

                it++;
            }

            q += u") WITH (TABLOCK)";

            batch b(n, no_check(q));
        }

        // FIXME - handle INT NULLs and VARCHAR NULLs

        return cols;
    }

    template<unsigned N>
    requires (N <= 38)
    class WARN_UNUSED numeric {
    public:
        numeric() = default;

        constexpr numeric(int64_t v) noexcept {
            if (v < 0)
                low_part = (uint64_t)-v;
            else
                low_part = (uint64_t)v;

            high_part = 0;
            neg = v < 0;

            for (unsigned int i = 0; i < N; i++) {
                ten_mult();
            }
        }

        constexpr numeric(uint64_t v) noexcept {
            low_part = v;
            high_part = 0;
            neg = false;

            for (unsigned int i = 0; i < N; i++) {
                ten_mult();
            }
        }

        template<typename T>
        requires std::is_integral_v<T> && std::is_signed_v<T>
        constexpr numeric(T v) noexcept : numeric(static_cast<int64_t>(v)) { }

        template<typename T>
        requires std::is_integral_v<T> && (!std::is_signed_v<T>)
        constexpr numeric(T v) noexcept : numeric(static_cast<uint64_t>(v)) { }

        numeric(std::floating_point auto d) noexcept {
            char buf[128];
            int exp;

            // FIXME - roundings?

            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), d, std::chars_format::fixed);

            std::string_view sv(buf, ptr - buf);
            std::string_view bef, aft;

            if (auto dot = sv.find('.'); dot != std::string_view::npos) {
                bef = sv.substr(0, dot);
                aft = sv.substr(dot + 1);
            } else
                bef = sv;

            neg = !bef.empty() && bef.front() == '-';

            if (neg)
                bef = bef.substr(1);

            if (bef.front() == '0')
                bef = bef.substr(1);

            if (bef.size() > 19) {
                exp = (int)(bef.size() - 19);
                bef = bef.substr(0, 19);
            } else {
                exp = 0;

                while (!aft.empty() && bef.size() < 19) {
                    char c = aft.front();

                    *((char*)bef.data() + bef.size()) = c;

                    bef = std::string_view(bef.data(), bef.size() + 1);

                    aft = aft.substr(1);
                    exp--;
                }
            }

            std::from_chars(&*bef.begin(), &*bef.end(), low_part);
            high_part = 0;

            for (int i = N; i < -exp; i++) {
                ten_div();
            }

            for (int i = -exp; i < (int)N; i++) {
                ten_mult();
            }
        }

        template<unsigned N2>
        constexpr numeric(const numeric<N2>& n) noexcept {
            low_part = n.low_part;
            high_part = n.high_part;
            neg = n.neg;

            if constexpr (N2 < N) {
                for (unsigned int i = N2; i < N; i++) {
                    ten_mult();
                }
            } else if constexpr (N2 > N) {
                for (unsigned int i = N; i < N2; i++) {
                    ten_div();
                }
            }
        }

        constexpr operator int64_t() noexcept {
            if constexpr (N == 0)
                return neg ? -(int64_t)low_part : (int64_t)low_part;
            else {
                numeric<0> n = *this;

                return neg ? -(int64_t)n.low_part : (int64_t)n.low_part;
            }
        }

        constexpr void ten_mult() noexcept {
            if (low_part >= std::numeric_limits<uint64_t>::max() / 10) {
                auto lp1 = low_part << 1;
                auto lp2 = low_part << 3;

                auto hp1 = low_part >> 63;
                auto hp2 = low_part >> 61;

                low_part = lp1 + lp2;
                high_part *= 10;
                high_part += hp1 + hp2;

                if ((lp1 >> 60) + (lp2 >> 60) >= 0x10)
                    high_part++;
            } else {
                low_part *= 10;
                high_part *= 10;
            }
        }

        constexpr void ten_div() noexcept {
            low_part /= 10;

            if (high_part != 0) {
                auto hp1 = high_part % 10;

                high_part /= 10;
                low_part += 0x199999999999999a * hp1;
            }
        }

        // FIXME - operator uint64_t?
        // FIXME - operator double
        // FIXME - operator numeric<N2>?

        constexpr std::strong_ordering operator<=>(const numeric<N>& n) const {
            if (neg && !n.neg)
                return std::strong_ordering::less;
            else if (!neg && n.neg)
                return std::strong_ordering::greater;

            if (high_part < n.high_part)
                return neg ? std::strong_ordering::greater : std::strong_ordering::less;
            else if (high_part > n.high_part)
                return neg ? std::strong_ordering::less : std::strong_ordering::greater;

            if (low_part < n.low_part)
                return neg ? std::strong_ordering::greater : std::strong_ordering::less;
            else if (low_part > n.low_part)
                return neg ? std::strong_ordering::less : std::strong_ordering::greater;

            return std::strong_ordering::equal;
        }

        template<unsigned N2>
        requires (N2 < N)
        constexpr std::partial_ordering operator<=>(const numeric<N2>& n) const {
            if (neg && !n.neg)
                return std::partial_ordering::less;
            else if (!neg && n.neg)
                return std::partial_ordering::greater;

            auto n2 = n;

            for (unsigned int i = N2; i < N; i++) {
                if (n2.high_part >= std::numeric_limits<uint64_t>::max() / 10)
                    return std::partial_ordering::unordered; // overflow

                n2.ten_mult();
            }

            if (high_part < n2.high_part)
                return neg ? std::partial_ordering::greater : std::partial_ordering::less;
            else if (high_part > n2.high_part)
                return neg ? std::partial_ordering::less : std::partial_ordering::greater;

            if (low_part < n2.low_part)
                return neg ? std::partial_ordering::greater : std::partial_ordering::less;
            else if (low_part > n2.low_part)
                return neg ? std::partial_ordering::less : std::partial_ordering::greater;

            return std::partial_ordering::equivalent;
        }

        uint64_t low_part, high_part;
        bool neg;
    };
};

template<>
struct std::formatter<enum tds::sql_type> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum tds::sql_type t, format_context& ctx) const {
        switch (t) {
            case tds::sql_type::IMAGE:
                return std::format_to(ctx.out(), "IMAGE");

            case tds::sql_type::TEXT:
                return std::format_to(ctx.out(), "TEXT");

            case tds::sql_type::UNIQUEIDENTIFIER:
                return std::format_to(ctx.out(), "UNIQUEIDENTIFIER");

            case tds::sql_type::INTN:
                return std::format_to(ctx.out(), "INTN");

            case tds::sql_type::DATE:
                return std::format_to(ctx.out(), "DATE");

            case tds::sql_type::TIME:
                return std::format_to(ctx.out(), "TIME");

            case tds::sql_type::DATETIME2:
                return std::format_to(ctx.out(), "DATETIME2");

            case tds::sql_type::DATETIMEOFFSET:
                return std::format_to(ctx.out(), "DATETIMEOFFSET");

            case tds::sql_type::SQL_VARIANT:
                return std::format_to(ctx.out(), "SQL_VARIANT");

            case tds::sql_type::NTEXT:
                return std::format_to(ctx.out(), "NTEXT");

            case tds::sql_type::BITN:
                return std::format_to(ctx.out(), "BITN");

            case tds::sql_type::DECIMAL:
                return std::format_to(ctx.out(), "DECIMAL");

            case tds::sql_type::NUMERIC:
                return std::format_to(ctx.out(), "NUMERIC");

            case tds::sql_type::FLTN:
                return std::format_to(ctx.out(), "FLTN");

            case tds::sql_type::MONEYN:
                return std::format_to(ctx.out(), "MONEYN");

            case tds::sql_type::DATETIMN:
                return std::format_to(ctx.out(), "DATETIMN");

            case tds::sql_type::VARBINARY:
                return std::format_to(ctx.out(), "VARBINARY");

            case tds::sql_type::VARCHAR:
                return std::format_to(ctx.out(), "VARCHAR");

            case tds::sql_type::BINARY:
                return std::format_to(ctx.out(), "BINARY");

            case tds::sql_type::CHAR:
                return std::format_to(ctx.out(), "CHAR");

            case tds::sql_type::NVARCHAR:
                return std::format_to(ctx.out(), "NVARCHAR");

            case tds::sql_type::NCHAR:
                return std::format_to(ctx.out(), "NCHAR");

            case tds::sql_type::UDT:
                return std::format_to(ctx.out(), "UDT");

            case tds::sql_type::XML:
                return std::format_to(ctx.out(), "XML");

            case tds::sql_type::SQL_NULL:
                return std::format_to(ctx.out(), "NULL");

            case tds::sql_type::TINYINT:
                return std::format_to(ctx.out(), "TINYINT");

            case tds::sql_type::BIT:
                return std::format_to(ctx.out(), "BIT");

            case tds::sql_type::SMALLINT:
                return std::format_to(ctx.out(), "SMALLINT");

            case tds::sql_type::INT:
                return std::format_to(ctx.out(), "INT");

            case tds::sql_type::DATETIM4:
                return std::format_to(ctx.out(), "DATETIM4");

            case tds::sql_type::REAL:
                return std::format_to(ctx.out(), "REAL");

            case tds::sql_type::MONEY:
                return std::format_to(ctx.out(), "MONEY");

            case tds::sql_type::DATETIME:
                return std::format_to(ctx.out(), "DATETIME");

            case tds::sql_type::FLOAT:
                return std::format_to(ctx.out(), "FLOAT");

            case tds::sql_type::SMALLMONEY:
                return std::format_to(ctx.out(), "SMALLMONEY");

            case tds::sql_type::BIGINT:
                return std::format_to(ctx.out(), "BIGINT");

            default:
                return std::format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<tds::datetime> {
    unsigned int len = 7;
    int len_arg = -1;

    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end()) {
            if (*it >= '0' && *it <= '7') {
                len = (unsigned int)(*it - '0');
                it++;
            } else if (*it == '{') {
                it++;

                if (it == ctx.end() || *it != '}')
                    throw format_error("invalid format");

                len_arg = (int)ctx.next_arg_id();

                it++;
            }
        }

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const tds::datetime& dt, format_context& ctx) const {
        auto len2 = len;
        auto hms = std::chrono::hh_mm_ss{dt.t};

        if (len_arg != -1) {
            auto arg = ctx.arg((size_t)len_arg);

            std::visit_format_arg([&](auto&& v) {
                if constexpr (std::is_integral_v<std::remove_reference_t<decltype(v)>>) {
                    len2 = (unsigned int)v;

                    if (len2 > 7)
                        throw format_error("size out of range");
                } else
                    throw format_error("invalid size argument");
            }, arg);
        }

        if (len2 == 0) {
            return std::format_to(ctx.out(), "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                            (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                            hms.hours().count(), hms.minutes().count(), hms.seconds().count());
        }

        double s = (double)hms.seconds().count() + ((double)hms.subseconds().count() / 10000000.0);

        return std::format_to(ctx.out(), "{:04}-{:02}-{:02} {:02}:{:02}:{:0{}.{}f}",
                         (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                         hms.hours().count(), hms.minutes().count(), s, len2 + 3, len2);
    }
};

template<>
struct std::formatter<tds::datetimeoffset> {
    unsigned int len = 7;
    int len_arg = -1;

    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end()) {
            if (*it >= '0' && *it <= '7') {
                len = (unsigned int)(*it - '0');
                it++;
            } else if (*it == '{') {
                it++;

                if (it == ctx.end() || *it != '}')
                    throw format_error("invalid format");

                len_arg = (int)ctx.next_arg_id();

                it++;
            }
        }

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const tds::datetimeoffset& dto, format_context& ctx) const {
        auto len2 = len;

        if (len_arg != -1) {
            auto arg = ctx.arg((size_t)len_arg);

            std::visit_format_arg([&](auto&& v) {
                if constexpr (std::is_integral_v<std::remove_reference_t<decltype(v)>>) {
                    len2 = (unsigned int)v;

                    if (len2 > 7)
                        throw format_error("size out of range");
                } else
                    throw format_error("invalid size argument");
            }, arg);
        }

        auto d = dto.d;
        auto t = dto.t;

        t += std::chrono::minutes{dto.offset};

        if (t < tds::time_t::zero()) {
            d = std::chrono::year_month_day{(std::chrono::sys_days)d - std::chrono::days{1}};
            t += std::chrono::days{1};
        } else if (t >= std::chrono::days{1}) {
            d = std::chrono::year_month_day{(std::chrono::sys_days)d + std::chrono::days{1}};
            t -= std::chrono::days{1};
        }

        auto hms = std::chrono::hh_mm_ss{t};

        if (len2 == 0) {
            return std::format_to(ctx.out(), "{:04}-{:02}-{:02} {:02}:{:02}:{:02} {:+03}:{:02}",
                             (int)d.year(), (unsigned int)d.month(), (unsigned int)d.day(),
                             hms.hours().count(), hms.minutes().count(), hms.seconds().count(),
                             dto.offset / 60, abs(dto.offset) % 60);
        }

        double s = (double)hms.seconds().count() + ((double)hms.subseconds().count() / 10000000.0);

        return std::format_to(ctx.out(), "{:04}-{:02}-{:02} {:02}:{:02}:{:0{}.{}f} {:+03}:{:02}",
                         (int)d.year(), (unsigned int)d.month(), (unsigned int)d.day(),
                         hms.hours().count(), hms.minutes().count(),
                         s, len2 + 3, len2,
                         dto.offset / 60, abs(dto.offset) % 60);
    }
};

template<>
struct std::formatter<tds::value> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const tds::value& p, format_context& ctx) const {
        if (p.is_null)
            return std::format_to(ctx.out(), "NULL");
        else if (p.type == tds::sql_type::VARBINARY || p.type == tds::sql_type::BINARY || p.type == tds::sql_type::IMAGE) {
            std::string s = "0x";

            for (auto c : p.val) {
                s += std::format("{:02x}", (uint8_t)c);
            }

            return std::format_to(ctx.out(), "{}", s);
        } else
            return std::format_to(ctx.out(), "{}", (std::string)p);
    }
};

template<typename T>
struct std::formatter<tds::output_param<T>> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const tds::output_param<T>& p, format_context& ctx) const {
        return std::format_to(ctx.out(), "{}", static_cast<tds::value>(p));
    }
};

template<>
struct std::formatter<tds::column> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const tds::column& c, format_context& ctx) const {
        return std::format_to(ctx.out(), "{}", static_cast<tds::value>(c));
    }
};

template<unsigned N>
struct std::formatter<tds::numeric<N>> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const tds::numeric<N>& n, format_context& ctx) const {
        uint8_t scratch[38];
        char s[80], *p;
        unsigned int pos;

        // double dabble

        *(uint64_t*)scratch = n.low_part;
        *(uint64_t*)&scratch[sizeof(uint64_t)] = n.high_part;
        memset(&scratch[2 * sizeof(uint64_t)], 0, sizeof(scratch) - (2 * sizeof(uint64_t)));

        for (unsigned int iter = 0; iter < 16 * 8; iter++) {
            for (unsigned int i = 16; i < 38; i++) {
                if (scratch[i] >> 4 >= 5) {
                    uint8_t v = scratch[i] >> 4;

                    v += 3;

                    scratch[i] = (uint8_t)((scratch[i] & 0xf) | (v << 4));
                }

                if ((scratch[i] & 0xf) >= 5) {
                    uint8_t v = scratch[i] & 0xf;

                    v += 3;

                    scratch[i] = (uint8_t)((scratch[i] & 0xf0) | v);
                }
            }

            bool carry = false;

            for (unsigned int i = 0; i < sizeof(scratch); i++) {
                bool b = scratch[i] & 0x80;

                scratch[i] <<= 1;

                if (carry)
                    scratch[i] |= 1;

                carry = b;
            }
        }

        p = s;
        pos = 0;
        for (unsigned int i = 37; i >= 16; i--) {
            *p = (char)((scratch[i] >> 4) + '0');
            p++;
            pos++;

            if (pos == 77 - (16 * 2) - N - 1) {
                *p = '.';
                p++;
            }

            *p = (char)((scratch[i] & 0xf) + '0');
            p++;
            pos++;

            if (pos == 77 - (16 * 2) - N - 1) {
                *p = '.';
                p++;
            }
        }
        *p = 0;

        auto dot = &s[77 - (16 * 2) - N - 1];

        // remove leading zeroes

        for (p = s; p < dot - 1; p++) {
            if (*p != '0')
                break;
        }

        if constexpr (N == 0) // remove trailing dot
            p[strlen(p) - 1] = 0;

        return std::format_to(ctx.out(), "{}{}", n.neg ? "-" : "", p);
    }
};

template<>
struct std::formatter<enum tds::token> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum tds::token t, format_context& ctx) const {
        switch (t) {
            case tds::token::OFFSET:
                return std::format_to(ctx.out(), "OFFSET");

            case tds::token::RETURNSTATUS:
                return std::format_to(ctx.out(), "RETURNSTATUS");

            case tds::token::COLMETADATA:
                return std::format_to(ctx.out(), "COLMETADATA");

            case tds::token::ALTMETADATA:
                return std::format_to(ctx.out(), "ALTMETADATA");

            case tds::token::DATACLASSIFICATION:
                return std::format_to(ctx.out(), "DATACLASSIFICATION");

            case tds::token::TABNAME:
                return std::format_to(ctx.out(), "TABNAME");

            case tds::token::COLINFO:
                return std::format_to(ctx.out(), "COLINFO");

            case tds::token::ORDER:
                return std::format_to(ctx.out(), "ORDER");

            case tds::token::TDS_ERROR:
                return std::format_to(ctx.out(), "ERROR");

            case tds::token::INFO:
                return std::format_to(ctx.out(), "INFO");

            case tds::token::RETURNVALUE:
                return std::format_to(ctx.out(), "RETURNVALUE");

            case tds::token::LOGINACK:
                return std::format_to(ctx.out(), "LOGINACK");

            case tds::token::FEATUREEXTACK:
                return std::format_to(ctx.out(), "FEATUREEXTACK");

            case tds::token::ROW:
                return std::format_to(ctx.out(), "ROW");

            case tds::token::NBCROW:
                return std::format_to(ctx.out(), "NBCROW");

            case tds::token::ALTROW:
                return std::format_to(ctx.out(), "ALTROW");

            case tds::token::ENVCHANGE:
                return std::format_to(ctx.out(), "ENVCHANGE");

            case tds::token::SESSIONSTATE:
                return std::format_to(ctx.out(), "SESSIONSTATE");

            case tds::token::SSPI:
                return std::format_to(ctx.out(), "SSPI");

            case tds::token::FEDAUTHINFO:
                return std::format_to(ctx.out(), "FEDAUTHINFO");

            case tds::token::DONE:
                return std::format_to(ctx.out(), "DONE");

            case tds::token::DONEPROC:
                return std::format_to(ctx.out(), "DONEPROC");

            case tds::token::DONEINPROC:
                return std::format_to(ctx.out(), "DONEINPROC");

            default:
                return std::format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<tds::collation> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const tds::collation& coll, format_context& ctx) const {
        return std::format_to(ctx.out(), "{}", coll.to_string());
    }
};

#ifdef _MSC_VER
#define pragma warning(pop)
#endif
