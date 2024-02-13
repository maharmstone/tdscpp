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
#include <charconv>

using namespace std;

static constexpr bool parse_time(string_view t, tds::time_t& dur, int16_t& offset) noexcept {
    uint8_t h = 0, m, s;
    uint32_t fracval = 0;

    {
        auto [ptr, ec] = from_chars(t.data(), t.data() + t.length(), h);

        if (ptr == t.data() || ptr - t.data() > 2 || h >= 24)
            return false;

        t = t.substr((size_t)(ptr - t.data()));
    }

    if (t.empty())
        return false;

    if (t.front() == ':') {
        t = t.substr(1);

        {
            auto [ptr, ec] = from_chars(t.data(), t.data() + t.length(), m);

            if (ptr == t.data() || ptr - t.data() > 2 || m >= 60)
                return false;

            t = t.substr((size_t)(ptr - t.data()));
        }

        if (!t.empty() && t.front() == ':') {
            t = t.substr(1);

            // hh:mm:ss.s and hh:mm:ss.s am

            {
                auto [ptr, ec] = from_chars(t.data(), t.data() + t.length(), s);

                if (ptr == t.data() || ptr - t.data() > 2 || s >= 60)
                    return false;

                t = t.substr((size_t)(ptr - t.data()));
            }

            if (!t.empty() && t.front() == '.') {
                t = t.substr(1);

                auto [ptr, ec] = from_chars(t.data(), t.data() + t.length(), fracval);

                if (ptr == t.data() || ptr - t.data() > 7)
                    return false;

                for (auto i = ptr - t.data(); i < 7; i++) {
                    fracval *= 10;
                }

                t = t.substr((size_t)(ptr - t.data()));
            }

            while (!t.empty() && t.front() == ' ') {
                t = t.substr(1);
            }

            if (t.length() >= 2 && (t[0] == 'A' || t[0] == 'a' || t[0] == 'P' || t[0] == 'p') && (t[1] == 'M' || t[1] == 'm')) {
                if ((t[0] == 'P' || t[0] == 'p') && h < 12)
                    h += 12;
                else if (h == 12 && (t[0] == 'A' || t[0] == 'a'))
                    h = 0;

                t = t.substr(2);
            }
        } else {
            while (!t.empty() && t.front() == ' ') {
                t = t.substr(1);
            }

            s = 0;

            if (t.length() >= 2 && (t[0] == 'A' || t[0] == 'a' || t[0] == 'P' || t[0] == 'p') && (t[1] == 'M' || t[1] == 'm')) { // hh:mm am
                if ((t[0] == 'P' || t[0] == 'p') && h < 12)
                    h += 12;
                else if (h == 12 && (t[0] == 'A' || t[0] == 'a'))
                    h = 0;

                t = t.substr(2);
            }

            // otherwise hh:mm
        }
    } else {
        while (!t.empty() && t.front() == ' ') {
            t = t.substr(1);
        }

        if (t.length() >= 2 && (t[0] == 'A' || t[0] == 'a' || t[0] == 'P' || t[0] == 'p') && (t[1] == 'M' || t[1] == 'm')) { // hh am
            m = 0;
            s = 0;

            if ((t[0] == 'P' || t[0] == 'p') && h < 12)
                h += 12;
            else if (h == 12 && (t[0] == 'A' || t[0] == 'a'))
                h = 0;

            t = t.substr(2);
        } else
            return false;
    }

    dur = chrono::hours{h} + chrono::minutes{m} + chrono::seconds{s} + tds::time_t{fracval};

    while (!t.empty() && (t.front() == ' ' || t.front() == '\t')) {
        t = t.substr(1);
    }

    if (t.empty()) {
        offset = 0;
        return true;
    }

    bool neg = false;

    if (t[0] == '-') {
        neg = true;
        t = t.substr(1);
    } else if (t[0] == '+')
        t = t.substr(1);

    if (t.empty())
        return false;

    uint16_t offset_hours, offset_mins;

    {
        auto [ptr, ec] = from_chars(t.data(), t.data() + t.length(), offset_hours);

        if (ptr == t.data())
            return false;

        t = t.substr((size_t)(ptr - t.data()));
    }

    if (t.empty() || t[0] != ':') {
        for (auto c : t) {
            if (c != ' ' && c != '\t')
                return false;
        }

        if (offset_hours >= 100) {
            offset_mins = offset_hours % 100;
            offset_hours /= 100;
        } else
            offset_mins = 0;

        if (offset_hours >= 24 || offset_mins >= 60)
            return false;

        offset = (int16_t)(((unsigned int)offset_hours * 60) + (unsigned int)offset_mins);

        if (neg)
            offset = -offset;

        return true;
    }

    if (offset_hours >= 24)
        return false;

    t = t.substr(1);

    {
        auto [ptr, ec] = from_chars(t.data(), t.data() + t.length(), offset_mins);

        if (ptr == t.data())
            return false;
    }

    if (offset_mins >= 60)
        return false;

    offset = (int16_t)(((unsigned int)offset_hours * 60) + (unsigned int)offset_mins);

    if (neg)
        offset = -offset;

    return true;
}

static constexpr bool test_parse_time(string_view t, bool exp_valid, tds::time_t exp_dur, int16_t exp_offset) {
    tds::time_t dur;
    int16_t offset;

    auto valid = parse_time(t, dur, offset);

    if (!exp_valid)
        return !valid;

    if (!valid)
        return false;

    return dur == exp_dur && offset == exp_offset;
}

static_assert(test_parse_time("", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("12", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("not a time value", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("12 aM", true, chrono::hours{0}, 0));
static_assert(test_parse_time("12pM", true, chrono::hours{12}, 0));
static_assert(test_parse_time("1 am", true, chrono::hours{1}, 0));
static_assert(test_parse_time("3 pm", true, chrono::hours{15}, 0));
static_assert(test_parse_time("01:23:45", true, chrono::hours{1} + chrono::minutes{23} + chrono::seconds{45}, 0));
static_assert(test_parse_time("01:23:45  PM", true, chrono::hours{13} + chrono::minutes{23} + chrono::seconds{45}, 0));
static_assert(test_parse_time("13:45", true, chrono::hours{13} + chrono::minutes{45}, 0));
static_assert(test_parse_time("1:45 Pm", true, chrono::hours{13} + chrono::minutes{45}, 0));
static_assert(test_parse_time("01:23:45.67", true, chrono::hours{1} + chrono::minutes{23} + chrono::seconds{45} + tds::time_t{6700000}, 0));
static_assert(test_parse_time("11:56:12.6789012  PM", true, chrono::hours{23} + chrono::minutes{56} + chrono::seconds{12} + tds::time_t{6789012}, 0));
static_assert(test_parse_time("01:23:45.67890123", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("01:23:60.6789012", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("01:60:45.6789012", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("24:23:45.6789012", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("12 aM +00:00", true, chrono::hours{0}, 0));
static_assert(test_parse_time("12pM -00:30", true, chrono::hours{12}, -30));
static_assert(test_parse_time("1 am +01:00", true, chrono::hours{1}, 60));
static_assert(test_parse_time("3 pm -01:30", true, chrono::hours{15}, -90));
static_assert(test_parse_time("01:23:45 +02:00", true, chrono::hours{1} + chrono::minutes{23} + chrono::seconds{45}, 120));
static_assert(test_parse_time("01:23:45  PM -02:30", true, chrono::hours{13} + chrono::minutes{23} + chrono::seconds{45}, -150));
static_assert(test_parse_time("13:45  +03:00", true, chrono::hours{13} + chrono::minutes{45}, 180));
static_assert(test_parse_time("1:45 Pm-03:30", true, chrono::hours{13} + chrono::minutes{45}, -210));
static_assert(test_parse_time("01:23:45.67+04:00", true, chrono::hours{1} + chrono::minutes{23} + chrono::seconds{45} + tds::time_t{6700000}, 240));
static_assert(test_parse_time("11:56:12.6789012  PM   -04:45", true, chrono::hours{23} + chrono::minutes{56} + chrono::seconds{12} + tds::time_t{6789012}, -285));
static_assert(test_parse_time("01:23:45.6789012 +00:60", false, tds::time_t::zero(), 0));
static_assert(test_parse_time("01:23:45.6789012 -24:00", false, tds::time_t::zero(), 0));

template<unsigned N>
static constexpr bool __inline string_match(string_view sv, const char (&str)[N]) noexcept {
    if (sv.length() < N - 1)
        return false;

    for (unsigned int i = 0; i < N - 1; i++) {
        if ((sv[i] | 0x20) != str[i])
            return false;
    }

    return true;
}

static constexpr uint8_t parse_month_name(string_view& sv) noexcept {
    if (sv.length() < 3)
        return 0;

    if (string_match(sv, "january")) {
        sv = sv.substr(7);
        return 1;
    } else if (string_match(sv, "february")) {
        sv = sv.substr(8);
        return 2;
    } else if (string_match(sv, "march")) {
        sv = sv.substr(5);
        return 3;
    } else if (string_match(sv, "april")) {
        sv = sv.substr(5);
        return 4;
    } else if (string_match(sv, "may")) {
        sv = sv.substr(3);
        return 5;
    } else if (string_match(sv, "june")) {
        sv = sv.substr(4);
        return 6;
    } else if (string_match(sv, "july")) {
        sv = sv.substr(4);
        return 7;
    } else if (string_match(sv, "august")) {
        sv = sv.substr(6);
        return 8;
    } else if (string_match(sv, "september")) {
        sv = sv.substr(9);
        return 9;
    } else if (string_match(sv, "october")) {
        sv = sv.substr(7);
        return 10;
    } else if (string_match(sv, "november")) {
        sv = sv.substr(8);
        return 11;
    } else if (string_match(sv, "december")) {
        sv = sv.substr(8);
        return 12;
    }

    if (sv.length() >= 3) {
        if (string_match(sv, "jan")) {
            sv = sv.substr(3);
            return 1;
        } else if (string_match(sv, "feb")) {
            sv = sv.substr(3);
            return 2;
        } else if (string_match(sv, "mar")) {
            sv = sv.substr(3);
            return 3;
        } else if (string_match(sv, "apr")) {
            sv = sv.substr(3);
            return 4;
        } else if (string_match(sv, "jun")) {
            sv = sv.substr(3);
            return 6;
        } else if (string_match(sv, "jul")) {
            sv = sv.substr(3);
            return 7;
        } else if (string_match(sv, "aug")) {
            sv = sv.substr(3);
            return 8;
        } else if (string_match(sv, "sep")) {
            sv = sv.substr(3);
            return 9;
        } else if (string_match(sv, "oct")) {
            sv = sv.substr(3);
            return 10;
        } else if (string_match(sv, "nov")) {
            sv = sv.substr(3);
            return 11;
        } else if (string_match(sv, "dec")) {
            sv = sv.substr(3);
            return 12;
        }
    }

    return 0;
}

static constexpr bool test_parse_month_name(string_view s, uint8_t exp) noexcept {
    return parse_month_name(s) == exp;
}

static_assert(test_parse_month_name("jUl", 7));
static_assert(test_parse_month_name("JuLy", 7));

static constexpr bool parse_date(string_view& s2, uint16_t& y, uint8_t& m, uint8_t& d) noexcept {
    if (s2.empty())
        return false;

    auto s = s2;

    if (s.front() >= '0' && s.front() <= '9') {
        uint32_t num;

        {
            auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)8), num);

            if (s.length() >= 8 && ptr == s.data() + 8) { // yyyymmdd
                y = (uint16_t)(num / 10000);
                m = (uint8_t)((num % 10000) / 100);
                d = (uint8_t)(num % 100);

                s = s.substr(8);
                s2 = s;
                return true;
            } else if (s.length() >= 6 && ptr == s.data() + 6) { // yyyymm[\\-/]dd
                s = s.substr(6);

                if (!s.empty() && (s.front() == '-' || s.front() == '/'))
                    s = s.substr(1);

                y = (uint16_t)(num / 100);
                m = (uint8_t)(num % 100);

                if (s.empty())
                    return false;

                auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)2), num);

                if (ptr == s.data())
                    return false;

                s = s.substr((size_t)(ptr - s.data()));

                d = (uint8_t)num;

                s2 = s;
                return true;
            } else if (s.length() >= 4 && ptr == s.data() + 4) { // yyyy[\\-/]mm[\\-/]dd
                s = s.substr(4);

                if (!s.empty() && (s.front() == '-' || s.front() == '/'))
                    s = s.substr(1);

                y = (uint16_t)num;

                if (s.empty())
                    return false;

                {
                    auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)2), m);

                    if (ptr == s.data())
                        return false;

                    s = s.substr((size_t)(ptr - s.data()));
                }

                if (!s.empty() && (s.front() == '-' || s.front() == '/'))
                    s = s.substr(1);

                if (s.empty())
                    return false;

                {
                    auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)2), d);

                    if (ptr == s.data())
                        return false;

                    s = s.substr((size_t)(ptr - s.data()));
                }

                s2 = s;

                return true;
            } else if (s.length() < 2 || (ptr != s.data() + 1 && ptr != s.data() + 2))
                return false;

            s = s.substr((size_t)(ptr - s.data()));
        }

        d = (uint8_t)num;

        if (!s.empty() && (s.front() == ' ' || s.front() == '-' || s.front() == '/'))
            s = s.substr(1);

        if (s.empty())
            return false;

        {
            auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)2), m);

            if (ptr == s.data()) {
                m = parse_month_name(s);

                if (m == 0)
                    return false;
            } else
                s = s.substr((size_t)(ptr - s.data()));
        }

        if (!s.empty() && (s.front() == ' ' || s.front() == '-' || s.front() == '/'))
            s = s.substr(1);

        if (s.empty())
            return false;

        auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)4), y);

        if (s.length() >= 4 && ptr == s.data() + 4) // dd/mm/yyyy
            s = s.substr(4);
        else if ((s.length() >= 1 && ptr == s.data() + 1) || (s.length() >= 2 && ptr == s.data() + 2)) {
            s = s.substr((size_t)(ptr - s.data()));

            if (y >= 50)
                y += 1900;
            else
                y += 2000;
        } else
            return false;

        s2 = s;

        return true;
    } else if ((s.front() >= 'A' && s.front() <= 'Z') || (s.front() >= 'a' && s.front() <= 'z')) {
        m = parse_month_name(s);

        if (m == 0)
            return false;

        if (!s.empty() && (s.front() == '-' || s.front() == '/' || s.front() == ' '))
            s = s.substr(1);

        uint16_t num;

        {
            auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)4), num);

            if (s.length() >= 4 && ptr == s.data() + 4) { // mon yyyy
                y = num;
                d = 1;
                s = s.substr(4);
                s2 = s;
                return true;
            }

            if (ptr == s.data()) // hide gcc maybe-unitialized warning
                return false;

            if (s.length() < 2 || (ptr != s.data() + 1 && ptr != s.data() + 2))
                return false;

            s = s.substr((size_t)(ptr - s.data()));
        }

        if (!s.empty() && s.front() == ',')
            s = s.substr(1);

        if (!s.empty() && (s.front() == '-' || s.front() == '/' || s.front() == ' '))
            s = s.substr(1);

        uint16_t num2;

        auto [ptr, ec] = from_chars(s.data(), s.data() + min(s.length(), (size_t)4), num2);

        switch (ptr - s.data()) {
            case 4:  // mon dd, yyyy
                y = num2;
                d = (uint8_t)num;
                s = s.substr(4);
                s2 = s;
                break;

            case 2: // mon dd, yy
                y = num2;
                d = (uint8_t)num;

                if (y >= 50)
                    y += 1900;
                else
                    y += 2000;

                s = s.substr(2);
                s2 = s;
                break;

            default: // mon yy
                y = num;
                d = 1;

                if (y >= 50)
                    y += 1900;
                else
                    y += 2000;

                break;
        }

        return true;
    } else
        return false;
}

static constexpr bool test_parse_date(string_view s, bool exp_valid, uint16_t exp_y, uint8_t exp_m, uint8_t exp_d) noexcept {
    uint16_t y;
    uint8_t m, d;
    bool valid;

    valid = parse_date(s, y, m, d);

    if (!valid)
        return !exp_valid;
    else if (!exp_valid)
        return false;

    return y == exp_y && m == exp_m && d == exp_d;
}

static_assert(test_parse_date("not a date", false, 0, 0, 0));
static_assert(test_parse_date("", false, 0, 0, 0));
static_assert(test_parse_date("2021-07-01", true, 2021, 7, 1));
static_assert(test_parse_date("2021/07/01", true, 2021, 7, 1));
static_assert(test_parse_date("20210701", true, 2021, 7, 1));
static_assert(test_parse_date("2021-7-1", true, 2021, 7, 1));
static_assert(test_parse_date("2021-7-01", true, 2021, 7, 1));
static_assert(test_parse_date("2021-07-1", true, 2021, 7, 1));
static_assert(test_parse_date("2021-0701", true, 2021, 7, 1));
static_assert(test_parse_date("202107-01", true, 2021, 7, 1));
static_assert(test_parse_date("202107-1", true, 2021, 7, 1));
static_assert(test_parse_date("2021/7/1", true, 2021, 7, 1));
static_assert(test_parse_date("2021/7/01", true, 2021, 7, 1));
static_assert(test_parse_date("2021/07/1", true, 2021, 7, 1));
static_assert(test_parse_date("2021/0701", true, 2021, 7, 1));
static_assert(test_parse_date("202107/01", true, 2021, 7, 1));
static_assert(test_parse_date("1/7/21", true, 2021, 7, 1));
static_assert(test_parse_date("01/7/21", true, 2021, 7, 1));
static_assert(test_parse_date("1/07/21", true, 2021, 7, 1));
static_assert(test_parse_date("01/07/21", true, 2021, 7, 1));
static_assert(test_parse_date("01-07-21", true, 2021, 7, 1));
static_assert(test_parse_date("1/7/2021", true, 2021, 7, 1));
static_assert(test_parse_date("01/7/2021", true, 2021, 7, 1));
static_assert(test_parse_date("1/07/2021", true, 2021, 7, 1));
static_assert(test_parse_date("01/07/2021", true, 2021, 7, 1));
static_assert(test_parse_date("01-07-2021", true, 2021, 7, 1));
static_assert(test_parse_date("1/jUl/21", true, 2021, 7, 1));
static_assert(test_parse_date("01/jUl/21", true, 2021, 7, 1));
static_assert(test_parse_date("1-jUl-21", true, 2021, 7, 1));
static_assert(test_parse_date("01-jUl-21", true, 2021, 7, 1));
static_assert(test_parse_date("1 jUl 21", true, 2021, 7, 1));
static_assert(test_parse_date("01 jUl 21", true, 2021, 7, 1));
static_assert(test_parse_date("1/JuLy/21", true, 2021, 7, 1));
static_assert(test_parse_date("01/JuLy/21", true, 2021, 7, 1));
static_assert(test_parse_date("01-JuLy-21", true, 2021, 7, 1));
static_assert(test_parse_date("1-JuLy-21", true, 2021, 7, 1));
static_assert(test_parse_date("01-JuLy-21", true, 2021, 7, 1));
static_assert(test_parse_date("1 JuLy 21", true, 2021, 7, 1));
static_assert(test_parse_date("01 JuLy 21", true, 2021, 7, 1));
static_assert(test_parse_date("1/jUl/2021", true, 2021, 7, 1));
static_assert(test_parse_date("01/jUl/2021", true, 2021, 7, 1));
static_assert(test_parse_date("1-jUl-2021", true, 2021, 7, 1));
static_assert(test_parse_date("01-jUl-2021", true, 2021, 7, 1));
static_assert(test_parse_date("1 jUl 2021", true, 2021, 7, 1));
static_assert(test_parse_date("01 jUl 2021", true, 2021, 7, 1));
static_assert(test_parse_date("1/JuLy/2021", true, 2021, 7, 1));
static_assert(test_parse_date("01/JuLy/2021", true, 2021, 7, 1));
static_assert(test_parse_date("01-JuLy-2021", true, 2021, 7, 1));
static_assert(test_parse_date("1-JuLy-2021", true, 2021, 7, 1));
static_assert(test_parse_date("01-JuLy-2021", true, 2021, 7, 1));
static_assert(test_parse_date("1 JuLy 2021", true, 2021, 7, 1));
static_assert(test_parse_date("01 JuLy 2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUL 21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 21", true, 2021, 7, 1));
static_assert(test_parse_date("jUL 2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 1,2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 1 2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy1-2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy1,2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-1,2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-1-2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-1,-2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 1,2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 1 2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl1-2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl1,2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-1,2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-1-2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-1,-2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 1,21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 1 21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy1-21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy1,21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-1,21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-1-21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-1,-21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 1,21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 1 21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl1-21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl1,21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-1,21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-1-21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-1,-21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 01,2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 01 2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy01-2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy01,2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-01,2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-01-2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-01,-2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 01,2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 01 2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl01-2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl01,2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-01,2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-01-2021", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-01,-2021", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 01,21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy 01 21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy01-21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy01,21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-01,21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-01-21", true, 2021, 7, 1));
static_assert(test_parse_date("JuLy-01,-21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 01,21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl 01 21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl01-21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl01,21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-01,21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-01-21", true, 2021, 7, 1));
static_assert(test_parse_date("jUl-01,-21", true, 2021, 7, 1));
static_assert(test_parse_date("2021-07-31", true, 2021, 7, 31));
static_assert(test_parse_date("2021/07/31", true, 2021, 7, 31));
static_assert(test_parse_date("20210731", true, 2021, 7, 31));
static_assert(test_parse_date("2021-7-31", true, 2021, 7, 31));
static_assert(test_parse_date("2021-0731", true, 2021, 7, 31));
static_assert(test_parse_date("202107-31", true, 2021, 7, 31));
static_assert(test_parse_date("2021/7/31", true, 2021, 7, 31));
static_assert(test_parse_date("2021/0731", true, 2021, 7, 31));
static_assert(test_parse_date("202107/31", true, 2021, 7, 31));
static_assert(test_parse_date("31/7/21", true, 2021, 7, 31));
static_assert(test_parse_date("31/07/21", true, 2021, 7, 31));
static_assert(test_parse_date("31-07-21", true, 2021, 7, 31));
static_assert(test_parse_date("31/7/2021", true, 2021, 7, 31));
static_assert(test_parse_date("31/07/2021", true, 2021, 7, 31));
static_assert(test_parse_date("31-07-2021", true, 2021, 7, 31));
static_assert(test_parse_date("31/jUl/21", true, 2021, 7, 31));
static_assert(test_parse_date("31-jUl-21", true, 2021, 7, 31));
static_assert(test_parse_date("31 jUl 21", true, 2021, 7, 31));
static_assert(test_parse_date("31/JuLy/21", true, 2021, 7, 31));
static_assert(test_parse_date("31-JuLy-21", true, 2021, 7, 31));
static_assert(test_parse_date("31 JuLy 21", true, 2021, 7, 31));
static_assert(test_parse_date("31/jUl/2021", true, 2021, 7, 31));
static_assert(test_parse_date("31-jUl-2021", true, 2021, 7, 31));
static_assert(test_parse_date("31 jUl 2021", true, 2021, 7, 31));
static_assert(test_parse_date("31/JuLy/2021", true, 2021, 7, 31));
static_assert(test_parse_date("31-JuLy-2021", true, 2021, 7, 31));
static_assert(test_parse_date("31 JuLy 2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy 31,2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy 31 2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy31-2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy31,2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy-31,2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy-31-2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy-31,-2021", true, 2021, 7, 31));
static_assert(test_parse_date("jUl 31,2021", true, 2021, 7, 31));
static_assert(test_parse_date("jUl 31 2021", true, 2021, 7, 31));
static_assert(test_parse_date("jUl31-2021", true, 2021, 7, 31));
static_assert(test_parse_date("jUl31,2021", true, 2021, 7, 31));
static_assert(test_parse_date("jUl-31,2021", true, 2021, 7, 31));
static_assert(test_parse_date("jUl-31-2021", true, 2021, 7, 31));
static_assert(test_parse_date("jUl-31,-2021", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy 31,21", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy 31 21", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy31-21", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy31,21", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy-31,21", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy-31-21", true, 2021, 7, 31));
static_assert(test_parse_date("JuLy-31,-21", true, 2021, 7, 31));
static_assert(test_parse_date("jUl 31,21", true, 2021, 7, 31));
static_assert(test_parse_date("jUl 31 21", true, 2021, 7, 31));
static_assert(test_parse_date("jUl31-21", true, 2021, 7, 31));
static_assert(test_parse_date("jUl31,21", true, 2021, 7, 31));
static_assert(test_parse_date("jUl-31,21", true, 2021, 7, 31));
static_assert(test_parse_date("jUl-31-21", true, 2021, 7, 31));
static_assert(test_parse_date("jUl-31,-21", true, 2021, 7, 31));

static constexpr bool is_valid_date(uint16_t y, uint8_t m, uint8_t d) {
    if (y == 0 || m == 0 || d == 0)
        return false;

    if (d > 31 || m > 12)
        return false;

    if (d == 31 && (m == 4 || m == 6 || m == 9 || m == 11))
        return false;

    if (d == 30 && m == 2)
        return false;

    if (d == 29 && m == 2) {
        if (y % 4)
            return false;

        if (!(y % 100) && y % 400)
            return false;
    }

    return true;
}

static_assert(!is_valid_date(0, 1, 1));
static_assert(!is_valid_date(1900, 0, 1));
static_assert(!is_valid_date(1900, 1, 0));
static_assert(is_valid_date(1900, 1, 1));
static_assert(!is_valid_date(1900, 2, 29));
static_assert(is_valid_date(2000, 2, 29));
static_assert(!is_valid_date(2000, 2, 30));
static_assert(!is_valid_date(2000, 13, 1));
static_assert(!is_valid_date(2000, 12, 32));
static_assert(!is_valid_date(2000, 6, 31));

static constexpr bool __inline is_digit(char c) noexcept {
    return c >= '0' && c <= '9';
}

static constexpr bool parse_datetime(string_view t, uint16_t& y, uint8_t& mon, uint8_t& d, tds::time_t& dur) noexcept {
    // ISO date
    if (t.length() >= 19 && is_digit(t[0]) && is_digit(t[1]) && is_digit(t[2]) && is_digit(t[3]) &&
        t[4] == '-' && is_digit(t[5]) && is_digit(t[6]) && t[7] == '-' && is_digit(t[8]) &&
        is_digit(t[9]) && t[10] == 'T' && is_digit(t[11]) && is_digit(t[12]) && t[13] == ':' &&
        is_digit(t[14]) && is_digit(t[15]) && t[16] == ':' && is_digit(t[17]) && is_digit(t[18])) {
        uint8_t h, mins, s;

        from_chars(t.data(), t.data() + 4, y);
        from_chars(t.data() + 5, t.data() + 7, mon);
        from_chars(t.data() + 8, t.data() + 10, d);
        from_chars(t.data() + 11, t.data() + 13, h);
        from_chars(t.data() + 14, t.data() + 16, mins);
        from_chars(t.data() + 17, t.data() + 19, s);

        if (!is_valid_date(y, mon, d) || h >= 24 || mins >= 60 || s >= 60)
            return false;

        t = t.substr(19);

        dur = chrono::hours{h} + chrono::minutes{mins} + chrono::seconds{s};

        if (t.empty())
            return true;

        if (t[0] == '.') {
            uint32_t v;

            t = t.substr(1);

            if (t.empty())
                return true;

            auto [ptr, ec] = from_chars(t.data(), t.data() + min(t.length(), (size_t)7), v);

            auto fraclen = (size_t)(ptr - t.data());

            if (fraclen == 0)
                return false;

            t = t.substr(fraclen);

            for (auto i = fraclen; i < 7; i++) {
                v *= 10;
            }

            if (!t.empty() && is_digit(t[0]))
                return false;

            dur += tds::time_t{v};

            if (t.empty())
                return true;
        }

        if (t.length() == 1 && t[0] == 'Z')
            return true;

        if (t[0] != '+' && t[0] != '-')
            return false;

        t = t.substr(1);

        if (t.length() < 5)
            return false;

        if (!is_digit(t[0]) || !is_digit(t[1]) || t[2] != ':' || !is_digit(t[3]) || !is_digit(t[4]))
            return false;

        return true;
    }

    if (parse_date(t, y, mon, d)) {
        if (!is_valid_date(y, mon, d))
            return false;

        if (t.empty()) {
            dur = tds::time_t::zero();
            return true;
        }

        if (t.front() != ' ' && t.front() != '\t')
            return false;

        while (t.front() == ' ' || t.front() == '\t') {
            t = t.substr(1);
        }

        int16_t offset;

        if (!parse_time(t, dur, offset))
            return false;

        return true;
    }

    // try to parse solo time

    int16_t offset;

    if (!parse_time(t, dur, offset))
        return false;

    y = 1900;
    mon = 1;
    d = 1;

    return true;
}

static constexpr bool test_parse_datetime(string_view t, bool exp_valid, uint16_t exp_y, uint8_t exp_mon,
                                          uint8_t exp_d, tds::time_t exp_dur) noexcept {
    bool valid;
    uint16_t y;
    uint8_t mon, d;
    tds::time_t dur;

    valid = parse_datetime(t, y, mon, d, dur);

    if (!valid)
        return !exp_valid;
    else if (!exp_valid)
        return false;

    return y == exp_y && mon == exp_mon && d == exp_d && dur == exp_dur;
}

static_assert(test_parse_datetime("not a datetime", false, 0, 0, 0, tds::time_t::zero()));
static_assert(test_parse_datetime("2021-07-02T10:05:34", true, 2021, 7, 2, 10h + 5min + 34s));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.1234567", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345678", false, 0, 0, 0, tds::time_t::zero()));
static_assert(test_parse_datetime("2021-07-02T10:05:34Z", true, 2021, 7, 2, 10h + 5min + 34s));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345Z", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.1234567Z", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345678Z", false, 0, 0, 0, tds::time_t::zero()));
static_assert(test_parse_datetime("2021-07-02T10:05:34+01:00", true, 2021, 7, 2, 10h + 5min + 34s));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345+01:00", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.1234567+01:00", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345678+01:00", false, 0, 0, 0, tds::time_t::zero()));
static_assert(test_parse_datetime("2021-07-02T10:05:34-12:34", true, 2021, 7, 2, 10h + 5min + 34s));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345-12:34", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.1234567-12:34", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}));
static_assert(test_parse_datetime("2021-07-02T10:05:34.12345678-12:34", false, 0, 0, 0, tds::time_t::zero()));
static_assert(test_parse_datetime("2021-07-02 10:05:34am", true, 2021, 7, 2, 10h + 5min + 34s));
static_assert(test_parse_datetime("July 2, 2021 10:05:34 AM", true, 2021, 7, 2, 10h + 5min + 34s));

static constexpr bool parse_datetimeoffset(string_view t, uint16_t& y, uint8_t& mon, uint8_t& d,
                                           tds::time_t& dur, int16_t& offset) noexcept {
    uint8_t h, mins, s;

    // ISO date
    if (t.length() >= 19 && is_digit(t[0]) && is_digit(t[1]) && is_digit(t[2]) && is_digit(t[3]) &&
        t[4] == '-' && is_digit(t[5]) && is_digit(t[6]) && t[7] == '-' && is_digit(t[8]) &&
        is_digit(t[9]) && t[10] == 'T' && is_digit(t[11]) && is_digit(t[12]) && t[13] == ':' &&
        is_digit(t[14]) && is_digit(t[15]) && t[16] == ':' && is_digit(t[17]) && is_digit(t[18])) {
        from_chars(t.data(), t.data() + 4, y);
        from_chars(t.data() + 5, t.data() + 7, mon);
        from_chars(t.data() + 8, t.data() + 10, d);
        from_chars(t.data() + 11, t.data() + 13, h);
        from_chars(t.data() + 14, t.data() + 16, mins);
        from_chars(t.data() + 17, t.data() + 19, s);

        if (!is_valid_date(y, mon, d) || h >= 24 || mins >= 60 || s >= 60)
            return false;

        t = t.substr(19);

        dur = chrono::hours{h} + chrono::minutes{mins} + chrono::seconds{s};

        if (t.empty()) {
            offset = 0;
            return true;
        }

        if (t[0] == '.') {
            uint32_t v;

            t = t.substr(1);

            if (t.empty()) {
                offset = 0;
                return true;
            }

            auto [ptr, ec] = from_chars(t.data(), t.data() + min(t.length(), (size_t)7), v);

            auto fraclen = (size_t)(ptr - t.data());

            if (fraclen == 0)
                return false;

            t = t.substr(fraclen);

            for (auto i = fraclen; i < 7; i++) {
                v *= 10;
            }

            if (!t.empty() && is_digit(t[0]))
                return false;

            dur += tds::time_t{v};

            if (t.empty()) {
                offset = 0;
                return true;
            }
        }

        if (t.length() == 1 && t[0] == 'Z') {
            offset = 0;
            return true;
        }

        if (t[0] != '+' && t[0] != '-')
            return false;

        auto offset_neg = t[0] == '-';

        t = t.substr(1);

        if (t.length() < 5)
            return false;

        unsigned int offset_hours, offset_mins;

        {
            auto [ptr, ec] = from_chars(t.data(), t.data() + 2, offset_hours);

            if (ptr != t.data() + 2)
                return false;
        }

        t = t.substr(2);

        if (t[0] != ':')
            return false;

        t = t.substr(1);

        {
            auto [ptr, ec] = from_chars(t.data(), t.data() + 2, offset_mins);

            if (ptr != t.data() + 2)
                return false;
        }

        if (offset_hours >= 24 || offset_mins >= 60)
            return false;

        offset = (int16_t)(offset_hours * 60);
        offset += (int16_t)offset_mins;

        if (offset_neg)
            offset = -offset;

        return true;
    }

    if (parse_date(t, y, mon, d)) {
        if (!is_valid_date(y, mon, d))
            return false;

        if (t.empty()) {
            dur = tds::time_t::zero();
            offset = 0;
            return true;
        }

        if (t.front() != ' ' && t.front() != '\t')
            return false;

        while (t.front() == ' ' || t.front() == '\t') {
            t = t.substr(1);
        }

        if (!parse_time(t, dur, offset) || offset <= -1440 || offset >= 1440)
            return false;

        return true;
    }

    // try to parse solo time

    if (!parse_time(t, dur, offset) || offset <= -1440 || offset >= 1440)
        return false;

    y = 1900;
    mon = 1;
    d = 1;

    return true;
}

static constexpr bool test_parse_datetimeoffset(string_view t, bool exp_valid, uint16_t exp_y, uint8_t exp_mon,
                                                uint8_t exp_d, tds::time_t exp_dur, int16_t exp_offset) noexcept {
    bool valid;
    uint16_t y;
    uint8_t mon, d;
    int16_t offset;
    tds::time_t dur;

    valid = parse_datetimeoffset(t, y, mon, d, dur, offset);

    if (!valid)
        return !exp_valid;
    else if (!exp_valid)
        return false;

    return y == exp_y && mon == exp_mon && d == exp_d && dur == exp_dur && offset == exp_offset;
}

static_assert(test_parse_datetimeoffset("not a datetimeoffset", false, 0, 0, 0, tds::time_t::zero(), 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34", true, 2021, 7, 2, 10h + 5min + 34s, 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}, 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.1234567", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}, 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345678", false, 0, 0, 0, tds::time_t::zero(), 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34Z", true, 2021, 7, 2, 10h + 5min + 34s, 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345Z", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}, 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.1234567Z", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}, 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345678Z", false, 0, 0, 0, tds::time_t::zero(), 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34+01:00", true, 2021, 7, 2, 10h + 5min + 34s, 60));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345-02:15", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}, -135));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.1234567+03:30", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}, 210));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345678-04:45", false, 0, 0, 0, tds::time_t::zero(), 0));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34+05:00", true, 2021, 7, 2, 10h + 5min + 34s, 300));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345-06:15", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234500}, -375));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.1234567+07:30", true, 2021, 7, 2, 10h + 5min + 34s + tds::time_t{1234567}, 450));
static_assert(test_parse_datetimeoffset("2021-07-02T10:05:34.12345678-08:45", false, 0, 0, 0, tds::time_t::zero(), 0));
static_assert(test_parse_datetimeoffset("2021-07-02 10:05:34am +09:00", true, 2021, 7, 2, 10h + 5min + 34s, 540));
static_assert(test_parse_datetimeoffset("July 2, 2021 10:05:34 AM -10:15", true, 2021, 7, 2, 10h + 5min + 34s, -615));

static unsigned int coll_to_cp(const tds::collation& coll) {
    if (coll.sort_id == 0) { // Windows collations
        switch (coll.lcid & 0xffff) {
            case 1054: // th-TH
                return 874;

            case 1041: // ja-JP
                return 932;

            case 2052: // zh-CN
                return 936;

            case 1042: // ko-KR
                return 949;

            case 1028: // zh-TW
            case 3076: // zh-HK
            case 5124: // zh-MO
                return 950;

            case 1029: // cs-CZ
            case 1038: // hu-HU
            case 1045: // pl-PL
            case 1048: // ro-RO
            case 1050: // hr-HR
            case 1051: // sk-SK
            case 1052: // sq-AL
            case 1060: // sl-SI
            case 1090: // tk-TM
            case 2074: // sr-Latn-CS
            case 5146: // bs-Latn-BA
                return 1250;

            case 1049: // ru-RU
            case 1058: // uk-UA
            case 1071: // mk-MK
            case 1087: // kk-KZ
            case 1092: // tt-RU
            case 1133: // ba-RU
            case 1157: // sah-RU
            case 2092: // az-Cyrl-AZ
            case 3098: // sr-Cyrl-CS
            case 8218: // bs-Cyrl-BA
                return 1251;

            case 1030: // da-DK
            case 1031: // de-DE
            case 1033: // en-US
            case 1034: // es-ES_tradnl
            case 1035: // fi-FI
            case 1036: // fr-FR
            case 1039: // is-IS
            case 1047: // rm-CH
            case 1044: // nb-NO
            case 1070: // hsb-DE
            case 1079: // ka-GE
            case 1083: // se-NO
            case 1106: // cy-GB
            case 1122: // fy-NL
            case 1146: // arn-CL
            case 1148: // moh-CA
            case 1150: // br-FR
            case 1155: // co-FR
            case 2107: // se-SE
            case 2143: // tzm-Latn-DZ
            case 3082: // es-ES
                return 1252;

            case 1032: // el-GR
                return 1253;

            case 1055: // tr-TR
            case 1068: // az-Latn-AZ
            case 1091: // uz-Latn-UZ
                return 1254;

            case 1037: // he-IL
                return 1255;

            case 1025: // ar-SA
            case 1056: // ur-PK
            case 1065: // fa-IR
            case 1152: // ug-CN
            case 1164: // prs-AF
                return 1256;

            case 1061: // et-EE
            case 1062: // lv-LV
            case 1063: // lt-LT
                return 1257;

            case 1066: // vi-VN
                return 1258;

            default:
                throw formatted_error("Could not map LCID {} to codepage.", coll.lcid);
        }
    } else { // SQL collations
        switch (coll.sort_id) {
            case 30:
            case 31:
            case 32:
            case 33:
            case 34:
                return 437;

            case 40:
            case 41:
            case 42:
            case 44:
            case 49:
            case 55:
            case 56:
            case 57:
            case 58:
            case 59:
            case 60:
            case 61:
                return 850;

            case 80:
            case 81:
            case 82:
            case 83:
            case 84:
            case 85:
            case 86:
            case 87:
            case 88:
            case 89:
            case 90:
            case 91:
            case 92:
            case 93:
            case 94:
            case 95:
            case 96:
                return 1250;

            case 104:
            case 105:
            case 106:
            case 107:
            case 108:
                return 1251;

            case 51:
            case 52:
            case 53:
            case 54:
            case 183:
            case 184:
            case 185:
            case 186:
                return 1252;

            case 112:
            case 113:
            case 114:
            case 121:
            case 124:
                return 1253;

            case 128:
            case 129:
            case 130:
                return 1254;

            case 136:
            case 137:
            case 138:
                return 1255;

            case 144:
            case 145:
            case 146:
                return 1256;

            case 152:
            case 153:
            case 154:
            case 155:
            case 156:
            case 157:
            case 158:
            case 159:
            case 160:
                return 1257;

            default:
                throw formatted_error("Could not map sort ID {} to codepage.", coll.sort_id);
        }
    }
}

namespace tds {
    static const auto jan1900 = -ymd_to_num({1y, chrono::January, 1d});

    value::value(const chrono::year_month_day& d) {
        int32_t n = ymd_to_num(d) + jan1900;

        type = sql_type::DATE;
        val.resize(3);

        memcpy(val.data(), &n, 3);
    }

    value::value(const optional<chrono::year_month_day>& d) {
        type = sql_type::DATE;

        if (!d.has_value())
            is_null = true;
        else {
            int32_t n = ymd_to_num(d.value()) + jan1900;
            val.resize(3);
            memcpy(val.data(), &n, 3);
        }
    }

    value::value(time_t t) {
        auto secs = (uint32_t)chrono::duration_cast<chrono::seconds>(t).count();

        type = sql_type::TIME;
        max_length = 0; // TIME(0)
        scale = 0;

        val.resize(3);
        memcpy(val.data(), &secs, val.size());
    }

    value::value(const std::optional<time_t>& t) {
        type = sql_type::TIME;
        max_length = 0; // TIME(0)
        scale = 0;

        if (!t.has_value())
            is_null = true;
        else {
            auto secs = (uint32_t)chrono::duration_cast<chrono::seconds>(t.value()).count();

            val.resize(3);
            memcpy(val.data(), &secs, val.size());
        }
    }

    value::value(const datetime& dt) {
        int32_t n;

        type = sql_type::DATETIME2;
        scale = 0;
        val.resize(8);
        max_length = 7; // DATETIME2(7)

        auto secs = (uint64_t)dt.t.count();

        memcpy(val.data(), &secs, 5);

        n = ymd_to_num(dt.d) + jan1900;
        memcpy(val.data() + 5, &n, 3);
    }

    value::value(const optional<datetime>& dt) {
        type = sql_type::DATETIME2;
        scale = 0;
        val.resize(8);
        max_length = 7; // DATETIME2(7)

        if (!dt.has_value())
            is_null = true;
        else {
            int32_t n;

            auto secs = (uint64_t)dt.value().t.count();

            memcpy(val.data(), &secs, 5);

            n = ymd_to_num(dt.value().d) + jan1900;
            memcpy(val.data() + 5, &n, 3);
        }
    }

    value::value(const datetimeoffset& dto) {
        int32_t n;

        type = sql_type::DATETIMEOFFSET;
        scale = 0;
        val.resize(10);
        max_length = 7; // DATETIMEOFFSET(7)

        auto ticks = (uint64_t)dto.t.count();

        memcpy(val.data(), &ticks, 5);

        n = ymd_to_num(dto.d) + jan1900;
        memcpy(val.data() + 5, &n, 3);

        *(int16_t*)(val.data() + 8) = dto.offset;
    }

    value::value(const optional<datetimeoffset>& dto) {
        type = sql_type::DATETIMEOFFSET;
        scale = 0;
        val.resize(10);
        max_length = 7; // DATETIMEOFFSET(7)

        if (!dto.has_value())
            is_null = true;
        else {
            int32_t n;

            auto ticks = (uint64_t)dto.value().t.count();

            memcpy(val.data(), &ticks, 5);

            n = ymd_to_num(dto.value().d) + jan1900;
            memcpy(val.data() + 5, &n, 3);

            *(int16_t*)(val.data() + 8) = dto.value().offset;
        }
    }

    static constexpr uint8_t bit_mask(uint8_t off, uint8_t len) {
        auto b = (uint8_t)((1 << (8 - off)) - 1);

        b = (uint8_t)(b & ~((1 << (8 - off - len)) - 1));

        return b;
    }

    static_assert(bit_mask(0, 1) == 0b10000000);
    static_assert(bit_mask(0, 6) == 0b11111100);
    static_assert(bit_mask(0, 7) == 0b11111110);
    static_assert(bit_mask(1, 1) == 0b01000000);
    static_assert(bit_mask(1, 6) == 0b01111110);
    static_assert(bit_mask(6, 2) == 0b00000011);

    static uint64_t read_bits(span<const uint8_t> sp, unsigned int off, unsigned int len) {
        uint64_t v = 0;

        sp = sp.subspan(off / 8);
        off %= 8;

        do {
            auto len2 = (uint8_t)min(len, 8 - off);

            uint64_t b = (sp.front() & bit_mask((uint8_t)off, len2)) >> (8 - off - len2);

            len -= len2;

            v |= b << len;

            if (len == 0)
                break;

            sp = sp.subspan(1);
            off += len2;
            off %= 8;
        } while (true);

        return v;
    }

    static string hierarchy_to_string(span<const uint8_t> d) {
        if (d.empty())
            return "/";

        unsigned int off = 0;
        auto bit_length = d.size() * 8;
        string ret = "/";

        do {
            auto l = read_bits(d, off, 6);
            int64_t o;

            if (l == 0b000000)
                break;

            switch (l) {
                case 0b000100: {
                    off += 6;
                    auto v = read_bits(d, off, 53);
                    off += 53;

                    o = (v & 0b11111111111111000000000000000000000000000000000000000) >> 5;
                    o |= (v & 0b11111111111111111111100000000000000000) >> 4;
                    o |= (v & 0b1111110000000000) >> 3;
                    o |= (v & 0b111000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o -= 281479271682120;

                    break;
                }

                case 0b000101: {
                    off += 6;
                    auto v = read_bits(d, off, 36);
                    off += 36;

                    o = (v & 0b111111111111111111100000000000000000) >> 4;
                    o |= (v & 0b1111110000000000) >> 3;
                    o |= (v & 0b111000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o -= 4294971464;

                    break;
                }

                case 0b000110: {
                    off += 6;
                    auto v = read_bits(d, off, 15);
                    off += 15;

                    o = (v & 0b111110000000000) >> 3;
                    o |= (v & 0b111000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o -= 4168;

                    break;
                }

                case 0b001000:
                case 0b001001:
                case 0b001010:
                case 0b001011: {
                    off += 4;
                    auto v = read_bits(d, off, 8);
                    off += 8;

                    o = (v & 0b11000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o -= 72;

                    break;
                }

                case 0b001110:
                case 0b001111:
                    off += 5;
                    o = (int64_t)(read_bits(d, off, 3) - 8);
                    off += 3;
                break;

                case 0b010000:
                case 0b010001:
                case 0b010010:
                case 0b010011:
                case 0b010100:
                case 0b010101:
                case 0b010110:
                case 0b010111:
                case 0b011000:
                case 0b011001:
                case 0b011010:
                case 0b011011:
                case 0b011100:
                case 0b011101:
                case 0b011110:
                case 0b011111:
                    off += 2;
                    o = (int64_t)read_bits(d, off, 2);
                    off += 2;
                    break;

                case 0b100000:
                case 0b100001:
                case 0b100010:
                case 0b100011:
                case 0b100100:
                case 0b100101:
                case 0b100110:
                case 0b100111:
                    off += 3;
                    o = (int64_t)(read_bits(d, off, 2) + 4);
                    off += 2;
                    break;

                case 0b101000:
                case 0b101001:
                case 0b101010:
                case 0b101011:
                case 0b101100:
                case 0b101101:
                case 0b101110:
                case 0b101111:
                    off += 3;
                    o = (int64_t)(read_bits(d, off, 3) + 8);
                    off += 3;
                    break;

                case 0b110000:
                case 0b110010:
                case 0b110100:
                case 0b110110: {
                    off += 3;
                    auto v = read_bits(d, off, 8);
                    off += 8;

                    o = (v & 0b11000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o += 16;

                    break;
                }

                case 0b111000:
                case 0b111001:
                case 0b111010:
                case 0b111011: {
                    off += 4;
                    auto v = read_bits(d, off, 13);
                    off += 13;

                    o = (v & 0b1110000000000) >> 3;
                    o |= (v & 0b111000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o += 80;

                    break;
                }

                case 0b111100:
                case 0b111101: {
                    off += 5;
                    auto v = read_bits(d, off, 15);
                    off += 15;

                    o = (v & 0b111110000000000) >> 3;
                    o |= (v & 0b111000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o += 1104;

                    break;
                }

                case 0b111110: {
                    off += 6;
                    auto v = read_bits(d, off, 36);
                    off += 36;

                    o = (v & 0b111111111111111111100000000000000000) >> 4;
                    o |= (v & 0b1111110000000000) >> 3;
                    o |= (v & 0b111000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o += 5200;

                    break;
                }

                case 0b111111: {
                    off += 6;
                    auto v = read_bits(d, off, 53);
                    off += 53;

                    o = (v & 0b11111111111111000000000000000000000000000000000000000) >> 5;
                    o |= (v & 0b11111111111111111111100000000000000000) >> 4;
                    o |= (v & 0b1111110000000000) >> 3;
                    o |= (v & 0b111000000) >> 2;
                    o |= (v & 0b10000) >> 1;
                    o |= v & 0b111;
                    o += 4294972496;

                    break;
                }

                default:
                    throw formatted_error("Unhandled bit sequence {:06b}", l);
            }

            auto f = read_bits(d, off, 1);
            off++;

            if (f)
                ret += format("{}/", o);
            else
                ret += format("{}.", o - 1);
        } while (off < bit_length - 6);

        return ret;
    }

    value::operator string() const {
        auto type2 = type;
        unsigned int max_length2 = max_length;
        uint8_t scale2 = scale;
        span d = val;

        if (is_null)
            return "";

        if (type2 == sql_type::SQL_VARIANT) {
            type2 = (sql_type)d[0];

            d = d.subspan(1);

            auto propbytes = d[0];

            d = d.subspan(1);

            switch (type2) {
                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                    max_length2 = d[0];
                break;

                case sql_type::NUMERIC:
                case sql_type::DECIMAL:
                    // ignore precision
                    scale2 = d[1];
                break;

                default:
                break;
            }

            d = d.subspan(propbytes);
        }

        switch (type2) {
            case sql_type::TINYINT:
                return format("{}", *(uint8_t*)d.data());

            case sql_type::SMALLINT:
                return format("{}", *(int16_t*)d.data());

            case sql_type::INT:
                return format("{}", *(int32_t*)d.data());

            case sql_type::BIGINT:
                return format("{}", *(int64_t*)d.data());

            case sql_type::INTN:
                switch (d.size()) {
                    case 1:
                        return format("{}", *(uint8_t*)d.data());

                    case 2:
                        return format("{}", *(int16_t*)d.data());

                    case 4:
                        return format("{}", *(int32_t*)d.data());

                    case 8:
                        return format("{}", *(int64_t*)d.data());

                    default:
                        throw formatted_error("INTN has unexpected length {}.", d.size());
                }
            break;

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::NTEXT:
            case sql_type::XML:
            {
                u16string_view sv((char16_t*)d.data(), d.size() / sizeof(char16_t));
                return utf16_to_utf8(sv);
            }

            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::TEXT:
            case sql_type::VARBINARY:
            case sql_type::BINARY:
            case sql_type::IMAGE:
                return string{string_view{(char*)d.data(), d.size()}};

            case sql_type::REAL:
                return format("{}", *(float*)d.data());

            case sql_type::FLOAT:
                return format("{}", *(double*)d.data());

            case sql_type::FLTN:
                switch (d.size()) {
                    case sizeof(float):
                        return format("{}", *(float*)d.data());

                    case sizeof(double):
                        return format("{}", *(double*)d.data());

                    default:
                        throw formatted_error("FLTN has unexpected length {}.", d.size());
                }
            break;

            case sql_type::DATE: {
                uint32_t v;

                memcpy(&v, d.data(), 3);
                v &= 0xffffff;

                auto dt = num_to_ymd((int)(v - jan1900));

                return format("{:04}-{:02}-{:02}", (int)dt.year(), (unsigned int)dt.month(), (unsigned int)dt.day());
            }

            case sql_type::TIME: {
                uint64_t ticks = 0;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size()));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                time_t t(ticks);
                chrono::hh_mm_ss hms(t);

                if (max_length2 == 0)
                    return format("{:02}:{:02}:{:02}", hms.hours().count(), hms.minutes().count(), hms.seconds().count());
                else {
                    double s = (double)hms.seconds().count() + ((double)hms.subseconds().count() / 10000000.0);

                    return format("{:02}:{:02}:{:0{}.{}f}", hms.hours().count(), hms.minutes().count(), s,
                                       max_length2 + 3, max_length2);
                }
            }

            case sql_type::DATETIME2: {
                uint64_t ticks = 0;
                uint32_t v;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 3));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                memcpy(&v, d.data() + d.size() - 3, 3);
                v &= 0xffffff;

                datetime dt(num_to_ymd((int)(v - jan1900)), time_t(ticks));

                return format("{:{}}", dt, max_length2);
            }

            case sql_type::DATETIME: {
                auto v = *(int32_t*)d.data();
                auto t = *(uint32_t*)(d.data() + sizeof(int32_t));
                auto dur = chrono::duration<int64_t, ratio<1, 300>>(t);

                datetime dt(num_to_ymd(v), dur);

                return format("{:3}", dt);
            }

            case sql_type::DATETIMN:
                switch (d.size()) {
                    case 4: {
                        auto v = *(uint16_t*)d.data();
                        auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));
                        auto ymd = num_to_ymd(v);
                        auto hms = std::chrono::hh_mm_ss{chrono::minutes(t)};

                        return format("{:04}-{:02}-{:02} {:02}:{:02}",
                                           (int)ymd.year(), (unsigned int)ymd.month(), (unsigned int)ymd.day(),
                                           hms.hours().count(), hms.minutes().count());
                    }

                    case 8: {
                        auto v = *(int32_t*)d.data();
                        auto t = *(uint32_t*)(d.data() + sizeof(int32_t));
                        auto dur = chrono::duration<int64_t, ratio<1, 300>>(t);

                        datetime dt(num_to_ymd(v), dur);

                        return format("{:3}", dt);
                    }

                    default:
                        throw formatted_error("DATETIMN has invalid length {}.", d.size());
                }

            case sql_type::DATETIM4: {
                auto v = *(uint16_t*)d.data();
                auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));
                auto ymd = num_to_ymd(v);
                auto hms = std::chrono::hh_mm_ss{chrono::minutes(t)};

                return format("{:04}-{:02}-{:02} {:02}:{:02}",
                                   (int)ymd.year(), (unsigned int)ymd.month(), (unsigned int)ymd.day(),
                                   hms.hours().count(), hms.minutes().count());
            }

            case sql_type::DATETIMEOFFSET: {
                uint64_t ticks = 0;
                uint32_t v;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 5));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                memcpy(&v, d.data() + d.size() - 5, 3);
                v &= 0xffffff;

                datetimeoffset dto;
                dto.d = num_to_ymd((int)(v - jan1900));
                dto.t = time_t{ticks};
                dto.offset = *(int16_t*)(d.data() + d.size() - sizeof(int16_t));

                return format("{:{}}", dto, max_length2);
            }

            case sql_type::BITN:
            case sql_type::BIT:
                return format("{}", d[0] != 0);

            case sql_type::NUMERIC:
            case sql_type::DECIMAL: {
                uint8_t scratch[38];
                char s[80], *p;
                unsigned int pos;
                auto numlen = (unsigned int)(d.size() - 1);

                // double dabble

                memcpy(scratch, d.data() + 1, d.size() - 1);
                memset(scratch + numlen, 0, sizeof(scratch) - numlen);

                for (unsigned int iter = 0; iter < numlen * 8; iter++) {
                    for (unsigned int i = numlen; i < 38; i++) {
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

                    buf_lshift<sizeof(scratch)>(scratch);
                }

                p = s;
                pos = 0;
                for (unsigned int i = 37; i >= numlen; i--) {
                    *p = (char)((scratch[i] >> 4) + '0');
                    p++;
                    pos++;

                    if (pos == 77 - (numlen * 2) - scale2 - 1) {
                        *p = '.';
                        p++;
                    }

                    *p = (char)((scratch[i] & 0xf) + '0');
                    p++;
                    pos++;

                    if (pos == 77 - (numlen * 2) - scale2 - 1) {
                        *p = '.';
                        p++;
                    }
                }
                *p = 0;

                auto dot = &s[77 - (numlen * 2) - scale2 - 1];

                // remove leading zeroes

                for (p = s; p < dot - 1; p++) {
                    if (*p != '0')
                        break;
                }

                if (scale2 == 0) // remove trailing dot
                    p[strlen(p) - 1] = 0;

                return format("{}{}", d[0] == 0 ? "-" : "", p);
            }

            case sql_type::MONEYN:
                switch (d.size()) {
                    case sizeof(int64_t): {
                        auto v = *(int64_t*)d.data();

                        v = (v >> 32) | ((v & 0xffffffff) << 32);

                        int16_t p = (int16_t)(v % 10000);

                        if (p < 0)
                            p = -p;

                        return format("{}.{:04}", v / 10000, p);
                    }

                    case sizeof(int32_t): {
                        auto v = *(int32_t*)d.data();

                        int16_t p = (int16_t)(v % 10000);

                        if (p < 0)
                            p = -p;

                        return format("{}.{:02}", v / 10000, p);
                    }

                    default:
                        throw formatted_error("MONEYN has unexpected length {}.", d.size());
                }

            case sql_type::MONEY: {
                auto v = *(int64_t*)d.data();

                v = (v >> 32) | ((v & 0xffffffff) << 32);

                int16_t p = (int16_t)(v % 10000);

                if (p < 0)
                    p = -p;

                return format("{}.{:04}", v / 10000, p);
            }

            case sql_type::SMALLMONEY: {
                auto v = *(int32_t*)d.data();

                int16_t p = (int16_t)(v % 10000);

                if (p < 0)
                    p = -p;

                return format("{}.{:02}", v / 10000, p);
            }

            case sql_type::UNIQUEIDENTIFIER:
                return format("{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                              *(uint32_t*)d.data(), *(uint16_t*)(d.data() + 4), *(uint16_t*)(d.data() + 6),
                              d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]);

            case sql_type::UDT:
                if (clr_name == u"Microsoft.SqlServer.Types.SqlHierarchyId, Microsoft.SqlServer.Types, Version=11.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91")
                    return hierarchy_to_string(d);

                throw formatted_error("Cannot convert UDT type {} to string", utf16_to_utf8(clr_name));

            default:
                throw formatted_error("Cannot convert {} to string", type2);
        }
    }

    value::operator u8string() const {
        if (is_null)
            return u8"";

        if (type == sql_type::NVARCHAR || type == sql_type::NCHAR || type == sql_type::NTEXT || type == sql_type::XML) {
            auto sv = u16string_view((char16_t*)val.data(), val.size() / sizeof(char16_t));
            u8string ret(utf16_to_utf8_len(sv), 0);

            utf16_to_utf8_range(sv, ret);

            return ret;
        } else if (type == sql_type::VARCHAR || type == sql_type::CHAR || type == sql_type::TEXT) {
            auto sv = string_view{(char*)val.data(), val.size()};

            if (coll.utf8 || (coll.lcid == 0 && coll.sort_id == 0))
                return u8string{u8string_view{(char8_t*)sv.data(), sv.length()}};

            auto cp = coll_to_cp(coll);
            auto u16s = cp_to_utf16(sv, cp);

            u8string ret(utf16_to_utf8_len(u16string_view{u16s}), 0);

            utf16_to_utf8_range(u16string_view{u16s}, ret);

            return ret;
        } else {
            auto s = operator string();

            // FIXME - is there any way to use move or swap here?

            return u8string{u8string_view{(char8_t*)s.data(), s.length()}};
        }
    }

    value::operator u16string() const {
        if (is_null)
            return u"";

        if (type == sql_type::NVARCHAR || type == sql_type::NCHAR || type == sql_type::NTEXT || type == sql_type::XML)
            return u16string(u16string_view((char16_t*)val.data(), val.size() / sizeof(char16_t)));
        else if (type == sql_type::VARCHAR || type == sql_type::CHAR || type == sql_type::TEXT) {
            auto sv = string_view{(char*)val.data(), val.size()};

            if (coll.utf8 || (coll.lcid == 0 && coll.sort_id == 0))
                return utf8_to_utf16(sv);

            auto cp = coll_to_cp(coll);

            return cp_to_utf16(sv, cp);
        } else
            return utf8_to_utf16(operator string());
    }

    value::operator int64_t() const {
        auto type2 = type;
        span d = val;

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
                return *(uint8_t*)d.data();

            case sql_type::SMALLINT:
                return *(int16_t*)d.data();

            case sql_type::INT:
                return *(int32_t*)d.data();

            case sql_type::BIGINT:
                return *(int64_t*)d.data();

            case sql_type::INTN:
                switch (d.size()) {
                    case 1:
                        return *(uint8_t*)d.data();

                    case 2:
                        return *(int16_t*)d.data();

                    case 4:
                        return *(int32_t*)d.data();

                    case 8:
                        return *(int64_t*)d.data();

                    default:
                        throw formatted_error("INTN has unexpected length {}.", d.size());
                }

            case sql_type::REAL:
                return (int64_t)*(float*)d.data();

            case sql_type::FLOAT:
                return (int64_t)*(double*)d.data();

            case sql_type::FLTN:
                switch (d.size()) {
                    case sizeof(float):
                        return (int64_t)*(float*)d.data();

                    case sizeof(double):
                        return (int64_t)*(double*)d.data();

                    default:
                        throw formatted_error("FLTN has unexpected length {}.", d.size());
                }

            case sql_type::BITN:
            case sql_type::BIT:
                return d[0] != 0 ? 1 : 0;

            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::TEXT:
            {
                if (d.empty())
                    return 0;

                bool first = true;

                auto sv = string_view((char*)d.data(), d.size());

                for (auto c : sv) {
                    if (c == '-') {
                        if (!first)
                            throw formatted_error("Cannot convert string \"{}\" to integer", sv);
                    } else if (c < '0' || c > '9')
                        throw formatted_error("Cannot convert string \"{}\" to integer", sv);

                    first = false;
                }

                int64_t res;

                auto [p, ec] = from_chars(sv.data(), sv.data() + sv.size(), res);

                if (ec == errc::invalid_argument)
                    throw formatted_error("Cannot convert string \"{}\" to integer", sv);
                else if (ec == errc::result_out_of_range)
                    throw formatted_error("String \"{}\" was too large to convert to BIGINT", sv);

                return res;
            }

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::NTEXT:
            {
                if (d.empty())
                    return 0;

                u16string_view v((char16_t*)d.data(), d.size() / sizeof(char16_t));
                string s;

                s.reserve(v.length());

                bool first = true;

                for (auto c : v) {
                    if (c == u'-') {
                        if (!first)
                            throw formatted_error("Cannot convert string \"{}\" to integer", utf16_to_utf8(v));
                    } else if (c < u'0' || c > u'9')
                        throw formatted_error("Cannot convert string \"{}\" to integer", utf16_to_utf8(v));

                    s += (char)c;
                    first = false;
                }

                int64_t res;

                auto [p, ec] = from_chars(s.data(), s.data() + s.length(), res);

                if (ec == errc::invalid_argument)
                    throw formatted_error("Cannot convert string \"{}\" to integer", s);
                else if (ec == errc::result_out_of_range)
                    throw formatted_error("String \"{}\" was too large to convert to BIGINT", s);

                return res;
            }

            case sql_type::DATETIME:
                return *(int32_t*)d.data(); // MSSQL adds 1 if after midday

            case sql_type::DATETIME2: {
                uint32_t n = 0;

                memcpy(&n, d.data() + d.size() - 3, 3);

                return (int32_t)n - jan1900;
            }

            case sql_type::DATETIMEOFFSET: {
                uint32_t n = 0;

                memcpy(&n, d.data() + d.size() - 5, 3);

                return (int32_t)n - jan1900;
            }

            case sql_type::DATETIMN:
                switch (d.size()) {
                    case 4:
                        return *(uint16_t*)d.data(); // MSSQL adds 1 if after midday

                    case 8:
                        return *(int32_t*)d.data(); // MSSQL adds 1 if after midday

                    default:
                        throw formatted_error("DATETIMN has invalid length {}", d.size());
                }

            case sql_type::DATETIM4:
                return *(uint16_t*)d.data(); // MSSQL adds 1 if after midday

            case sql_type::NUMERIC:
            case sql_type::DECIMAL:
            {
                if (d.empty())
                    return 0;

                bool first = true;
                auto s = (string)*this;

                for (auto c : s) {
                    if (c == '-') {
                        if (!first)
                            throw formatted_error("Cannot convert {} to integer", s);
                    } else if (c == '.')
                        break;
                    else if (c < '0' || c > '9')
                        throw formatted_error("Cannot convert {} to integer", s);

                    first = false;
                }

                int64_t res;

                auto [p, ec] = from_chars(s.data(), s.data() + s.length(), res);

                if (ec == errc::invalid_argument)
                    throw formatted_error("Cannot convert {} to integer", s);
                else if (ec == errc::result_out_of_range)
                    throw formatted_error("{} was too large to convert to BIGINT", s);

                return res;
            }

            case sql_type::MONEYN:
                switch (d.size()) {
                    case sizeof(int64_t): {
                        auto v = *(int64_t*)d.data();

                        v = (v >> 32) | ((v & 0xffffffff) << 32);

                        return v / 10000;
                    }

                    case sizeof(int32_t): {
                        auto v = *(int32_t*)d.data();

                        return v / 10000;
                    }

                    default:
                        throw formatted_error("MONEYN has unexpected length {}", d.size());
                }

            case sql_type::MONEY: {
                auto v = *(int64_t*)d.data();

                v = (v >> 32) | ((v & 0xffffffff) << 32);

                return v / 10000;
            }

            case sql_type::SMALLMONEY: {
                auto v = *(int32_t*)d.data();

                return v / 10000;
            }

            // MSSQL doesn't allow conversion to INT for DATE, TIME, DATETIME2, or DATETIMEOFFSET

            // Not allowing VARBINARY even though MSSQL does

            default:
                throw formatted_error("Cannot convert {} to integer", type2);
        }
    }

    value::operator chrono::year_month_day() const {
        auto type2 = type;
        span d = val;

        if (is_null)
            return chrono::year_month_day{1900y, chrono::January, 1d};

        if (type2 == sql_type::SQL_VARIANT) {
            type2 = (sql_type)d[0];

            d = d.subspan(1);

            auto propbytes = d[0];

            d = d.subspan(1 + propbytes);
        }

        switch (type2) {
            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::TEXT:
            {
                uint16_t y;
                uint8_t mon, day;

                auto t = string_view((char*)d.data(), d.size());

                // remove leading whitespace

                while (!t.empty() && (t.front() == ' ' || t.front() == '\t')) {
                    t = t.substr(1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == ' ' || t.back() == '\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return chrono::year_month_day{1900y, chrono::January, 1d};

                time_t dur;

                if (!parse_datetime(t, y, mon, day, dur) || !is_valid_date(y, mon, day))
                    throw formatted_error("Cannot convert string \"{}\" to date", string_view((char*)d.data(), d.size()));

                return chrono::year_month_day{chrono::year{y}, chrono::month{mon}, chrono::day{day}};
            }

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::NTEXT:
            {
                uint16_t y;
                uint8_t mon, day;

                auto t = u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t));

                // remove leading whitespace

                while (!t.empty() && (t.front() == u' ' || t.front() == u'\t')) {
                    t = t.substr(1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == u' ' || t.back() == u'\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return chrono::year_month_day{1900y, chrono::January, 1d};

                string t2;

                t2.reserve(t.length());

                for (auto c : t) {
                    t2 += (char)c;
                }

                auto sv = string_view(t2);
                time_t dur;

                if (!parse_datetime(sv, y, mon, day, dur) || !is_valid_date(y, mon, day))
                    throw formatted_error("Cannot convert string \"{}\" to date", utf16_to_utf8(u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t))));

                return chrono::year_month_day{chrono::year{y}, chrono::month{mon}, chrono::day{day}};
            }

            case sql_type::DATE: {
                uint32_t n = 0;

                memcpy(&n, d.data(), 3);

                return num_to_ymd((int)(n - jan1900));
            }

            case sql_type::DATETIME:
                return num_to_ymd(*(int32_t*)d.data());

            case sql_type::DATETIMN:
                switch (d.size()) {
                    case 4:
                        return num_to_ymd(*(uint16_t*)d.data());

                    case 8:
                        return num_to_ymd(*(int32_t*)d.data());

                    default:
                        throw formatted_error("DATETIMN has invalid length {}", d.size());
                }

            case sql_type::DATETIM4:
                return num_to_ymd(*(uint16_t*)d.data());

            case sql_type::DATETIME2: {
                uint32_t n = 0;

                memcpy(&n, d.data() + d.size() - 3, 3);

                return num_to_ymd((int32_t)n - jan1900);
            }

            case sql_type::DATETIMEOFFSET: {
                uint32_t n = 0;

                memcpy(&n, d.data() + d.size() - 5, 3);

                return num_to_ymd((int32_t)n - jan1900);
            }

            // MSSQL doesn't allow conversion to DATE for integers, floats, BITs, or TIME

            case sql_type::TINYINT:
            case sql_type::SMALLINT:
            case sql_type::INT:
            case sql_type::BIGINT:
            case sql_type::INTN: {
                auto n = (int64_t)*this;

                throw formatted_error("Cannot convert integer {} to std::chrono::year_month_day", n);
            }

            default:
                throw formatted_error("Cannot convert {} to std::chrono::year_month_day", type2);
        }
    }

    value::operator time_t() const {
        auto type2 = type;
        unsigned int max_length2 = max_length;
        span d = val;

        if (is_null)
            return time_t::zero();

        if (type2 == sql_type::SQL_VARIANT) {
            type2 = (sql_type)d[0];

            d = d.subspan(1);

            auto propbytes = (uint8_t)d[0];

            d = d.subspan(1);

            switch (type2) {
                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                    max_length2 = d[0];
                    break;

                default:
                    break;
            }

            d = d.subspan(propbytes);
        }

        switch (type2) {
            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::TEXT:
            {
                uint16_t y;
                uint8_t mon, day;

                auto t = string_view((char*)d.data(), d.size());

                // remove leading whitespace

                while (!t.empty() && (t.front() == ' ' || t.front() == '\t')) {
                    t = t.substr(1, t.length() - 1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == ' ' || t.back() == '\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return time_t::zero();

                time_t dur;

                if (!parse_datetime(t, y, mon, day, dur))
                    throw formatted_error("Cannot convert string \"{}\" to time", string_view((char*)d.data(), d.size()));

                return dur;
            }

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::NTEXT:
            {
                uint16_t y;
                uint8_t mon, day;

                auto t = u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t));

                // remove leading whitespace

                while (!t.empty() && (t.front() == u' ' || t.front() == u'\t')) {
                    t = t.substr(1, t.length() - 1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == u' ' || t.back() == u'\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return time_t::zero();

                string t2;

                t2.reserve(t.length());

                for (auto c : t) {
                    t2 += (char)c;
                }

                time_t dur;

                if (!parse_datetime(t2, y, mon, day, dur))
                    throw formatted_error("Cannot convert string \"{}\" to time", utf16_to_utf8(u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t))));

                return dur;
            }

            case sql_type::TIME: {
                uint64_t ticks = 0;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size()));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return time_t(ticks);
            }

            case sql_type::DATETIME: {
                auto v = *(uint32_t*)(d.data() + sizeof(int32_t));
                auto dur = chrono::duration<int64_t, ratio<1, 300>>(v);

                return chrono::duration_cast<time_t>(dur);
            }

            case sql_type::DATETIMN:
                switch (d.size()) {
                    case 4: {
                        auto v = *(uint16_t*)(d.data() + sizeof(uint16_t));
                        auto dur = chrono::minutes(v);

                        return chrono::duration_cast<time_t>(dur);
                    }

                    case 8: {
                        auto v = *(uint32_t*)(d.data() + sizeof(int32_t));
                        auto dur = chrono::duration<int64_t, ratio<1, 300>>(v);

                        return chrono::duration_cast<time_t>(dur);
                    }

                    default:
                        throw formatted_error("DATETIMN has invalid length {}", d.size());
                }

            case sql_type::DATETIM4: {
                auto v = *(uint16_t*)(d.data() + sizeof(uint16_t));
                auto dur = chrono::minutes(v);

                return chrono::duration_cast<time_t>(dur);
            }

            case sql_type::DATETIME2: {
                uint64_t ticks = 0;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 3));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return time_t(ticks);
            }

            case sql_type::DATETIMEOFFSET: {
                uint64_t ticks = 0;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 5));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return time_t(ticks);
            }

            // MSSQL doesn't allow conversion to TIME for integers, floats, BITs, or DATE

            default:
                throw formatted_error("Cannot convert {} to std::chrono::duration", type2);
        }
    }

    value::operator datetime() const {
        auto type2 = type;
        unsigned int max_length2 = max_length;
        span d = val;

        if (is_null)
            return datetime{1900y, chrono::January, 1d, 0, 0, 0};

        if (type2 == sql_type::SQL_VARIANT) {
            type2 = (sql_type)d[0];

            d = d.subspan(1);

            auto propbytes = d[0];

            d = d.subspan(1);

            switch (type2) {
                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                    max_length2 = d[0];
                    break;

                default:
                    break;
            }

            d = d.subspan(propbytes);
        }

        switch (type2) {
            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::TEXT:
            {
                uint16_t y;
                uint8_t mon, day;

                auto t = string_view((char*)d.data(), d.size());

                // remove leading whitespace

                while (!t.empty() && (t.front() == ' ' || t.front() == '\t')) {
                    t = t.substr(1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == ' ' || t.back() == '\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return datetime{1900y, chrono::January, 1d, 0, 0, 0};

                time_t dur;

                if (!parse_datetime(t, y, mon, day, dur))
                    throw formatted_error("Cannot convert string \"{}\" to datetime", string_view((char*)d.data(), d.size()));

                return datetime{chrono::year{y}, chrono::month{mon}, chrono::day{day}, dur};
            }

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::NTEXT:
            {
                uint16_t y;
                uint8_t mon, day;

                auto t = u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t));

                // remove leading whitespace

                while (!t.empty() && (t.front() == u' ' || t.front() == u'\t')) {
                    t = t.substr(1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == u' ' || t.back() == u'\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return datetime{1900y, chrono::January, 1d, 0, 0, 0};

                string t2;

                t2.reserve(t.length());

                for (auto c : t) {
                    t2 += (char)c;
                }

                time_t dur;

                if (!parse_datetime(t2, y, mon, day, dur))
                    throw formatted_error("Cannot convert string \"{}\" to datetime", utf16_to_utf8(u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t))));

                return datetime{chrono::year{y}, chrono::month{mon}, chrono::day{day}, dur};
            }

            case sql_type::DATE: {
                uint32_t n = 0;

                memcpy(&n, d.data(), 3);

                return datetime{num_to_ymd((int)(n - jan1900)), time_t(0)};
            }

            case sql_type::TIME: {
                uint64_t ticks = 0;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size()));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return datetime{chrono::year_month_day{1900y, chrono::January, 1d}, time_t(ticks)};
            }

            case sql_type::DATETIME: {
                auto v = *(int32_t*)d.data();
                auto t = *(uint32_t*)(d.data() + sizeof(int32_t));
                auto dur = chrono::duration<int64_t, ratio<1, 300>>(t);

                return datetime{num_to_ymd(v), dur};
            }

            case sql_type::DATETIMN:
                switch (d.size()) {
                    case 4: {
                        auto v = *(uint16_t*)d.data();
                        auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));
                        auto dur = chrono::minutes(t);

                        return datetime{num_to_ymd(v), dur};
                    }

                    case 8: {
                        auto v = *(int32_t*)d.data();
                        auto t = *(uint32_t*)(d.data() + sizeof(int32_t));
                        auto dur = chrono::duration<int64_t, ratio<1, 300>>(t);

                        return datetime{num_to_ymd(v), dur};
                    }

                    default:
                        throw formatted_error("DATETIMN has invalid length {}", d.size());
                }

            case sql_type::DATETIM4: {
                auto v = *(uint16_t*)d.data();
                auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));
                auto dur = chrono::minutes(t);

                return datetime{num_to_ymd(v), dur};
            }

            case sql_type::DATETIME2: {
                uint32_t n = 0;
                uint64_t ticks = 0;

                memcpy(&n, d.data() + d.size() - 3, 3);

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 3));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return datetime{num_to_ymd((int32_t)n - jan1900), time_t(ticks)};
            }

            case sql_type::DATETIMEOFFSET: {
                uint32_t n = 0;
                uint64_t ticks = 0;

                memcpy(&n, d.data() + d.size() - 5, 3);

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 5));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return datetime{num_to_ymd((int32_t)n - jan1900), time_t(ticks)};
            }

            // MSSQL doesn't allow conversion to DATETIME2 for integers, floats, or BIT

            default:
                throw formatted_error("Cannot convert {} to datetime", type2);
        }
    }

    value::operator datetimeoffset() const {
        auto type2 = type;
        unsigned int max_length2 = max_length;
        span d = val;

        if (is_null)
            return datetimeoffset{1900y, chrono::January, 1d, 0, 0, 0, 0};

        if (type2 == sql_type::SQL_VARIANT) {
            type2 = (sql_type)d[0];

            d = d.subspan(1);

            auto propbytes = d[0];

            d = d.subspan(1);

            switch (type2) {
                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                    max_length2 = d[0];
                    break;

                default:
                    break;
            }

            d = d.subspan(propbytes);
        }

        switch (type2) {
            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::TEXT:
            {
                uint16_t y;
                uint8_t mon, day;
                int16_t offset;

                auto t = string_view((char*)d.data(), d.size());

                // remove leading whitespace

                while (!t.empty() && (t.front() == ' ' || t.front() == '\t')) {
                    t = t.substr(1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == ' ' || t.back() == '\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return datetimeoffset{1900y, chrono::January, 1d, 0, 0, 0, 0};

                time_t dur;

                if (!parse_datetimeoffset(t, y, mon, day, dur, offset))
                    throw formatted_error("Cannot convert string \"{}\" to datetimeoffset", string_view((char*)d.data(), d.size()));

                return datetimeoffset{chrono::year{y}, chrono::month{mon}, chrono::day{day}, dur, offset};
            }

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::NTEXT:
            {
                uint16_t y;
                uint8_t mon, day;
                int16_t offset;

                auto t = u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t));

                // remove leading whitespace

                while (!t.empty() && (t.front() == u' ' || t.front() == u'\t')) {
                    t = t.substr(1);
                }

                // remove trailing whitespace

                while (!t.empty() && (t.back() == u' ' || t.back() == u'\t')) {
                    t = t.substr(0, t.length() - 1);
                }

                if (t.empty())
                    return datetimeoffset{1900y, chrono::January, 1d, 0, 0, 0, 0};

                string t2;

                t2.reserve(t.length());

                for (auto c : t) {
                    t2 += (char)c;
                }

                time_t dur;

                if (!parse_datetimeoffset(t2, y, mon, day, dur, offset))
                    throw formatted_error("Cannot convert string \"{}\" to datetimeoffset", utf16_to_utf8(u16string_view((char16_t*)d.data(), d.size() / sizeof(char16_t))));

                return datetimeoffset{chrono::year{y}, chrono::month{mon}, chrono::day{day}, dur, offset};
            }

            case sql_type::DATE: {
                uint32_t n = 0;

                memcpy(&n, d.data(), 3);

                return datetimeoffset{num_to_ymd((int)(n - jan1900)), time_t(0), 0};
            }

            case sql_type::TIME: {
                uint64_t ticks = 0;

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size()));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return datetimeoffset{chrono::year_month_day{1900y, chrono::January, 1d}, time_t(ticks), 0};
            }

            case sql_type::DATETIME: {
                auto v = *(int32_t*)d.data();
                auto t = *(uint32_t*)(d.data() + sizeof(int32_t));
                auto dur = chrono::duration<int64_t, ratio<1, 300>>(t);

                return datetimeoffset{num_to_ymd(v), dur, 0};
            }

            case sql_type::DATETIMN:
                switch (d.size()) {
                    case 4: {
                        auto v = *(uint16_t*)d.data();
                        auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));
                        auto dur = chrono::minutes(t);

                        return datetimeoffset{num_to_ymd(v), dur, 0};
                    }

                    case 8: {
                        auto v = *(int32_t*)d.data();
                        auto t = *(uint32_t*)(d.data() + sizeof(int32_t));
                        auto dur = chrono::duration<int64_t, ratio<1, 300>>(t);

                        return datetimeoffset{num_to_ymd(v), dur, 0};
                    }

                    default:
                        throw formatted_error("DATETIMN has invalid length {}", d.size());
                }

            case sql_type::DATETIM4: {
                auto v = *(uint16_t*)d.data();
                auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));
                auto dur = chrono::minutes(t);

                return datetimeoffset{num_to_ymd(v), dur, 0};
            }

            case sql_type::DATETIME2: {
                uint32_t n = 0;
                uint64_t ticks = 0;

                memcpy(&n, d.data() + d.size() - 3, 3);

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 3));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                return datetimeoffset{num_to_ymd((int32_t)n - jan1900), time_t(ticks), 0};
            }

            case sql_type::DATETIMEOFFSET: {
                uint32_t n = 0;
                uint64_t ticks = 0;

                memcpy(&n, d.data() + d.size() - 5, 3);

                memcpy(&ticks, d.data(), min(sizeof(uint64_t), d.size() - 5));

                for (unsigned int n = 0; n < 7 - max_length2; n++) {
                    ticks *= 10;
                }

                datetimeoffset dto;

                dto.d = num_to_ymd((int32_t)n - jan1900);
                dto.t = time_t(ticks);
                dto.offset = *(int16_t*)&d[d.size() - sizeof(int16_t)];

                return dto;
            }

            // MSSQL doesn't allow conversion to DATETIME2 for integers, floats, or BIT

            default:
                throw formatted_error("Cannot convert {} to datetimeoffset", type2);
        }
    }

    value::operator double() const {
        auto type2 = type;
        auto max_length2 = max_length;
        span d = val;

        if (is_null)
            return 0;

        if (type2 == sql_type::SQL_VARIANT) {
            type2 = (sql_type)d[0];

            d = d.subspan(1);

            auto propbytes = d[0];

            d = d.subspan(1);

            switch (type2) {
                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                    max_length2 = d[0];
                    break;

                default:
                    break;
            }

            d = d.subspan(propbytes);
        }

        switch (type2) {
            case sql_type::TINYINT:
            case sql_type::SMALLINT:
            case sql_type::INT:
            case sql_type::BIGINT:
            case sql_type::INTN:
            case sql_type::BITN:
            case sql_type::BIT:
                return (double)operator int64_t();

            case sql_type::REAL:
                return *(float*)d.data();

            case sql_type::FLOAT:
                return *(double*)d.data();

            case sql_type::FLTN:
                switch (d.size()) {
                    case sizeof(float):
                        return *(float*)d.data();

                    case sizeof(double):
                        return *(double*)d.data();

                    default:
                        throw formatted_error("FLTN has unexpected length {}", d.size());
                }

            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::TEXT:
            {
                if (d.empty())
                    return 0.0;

                auto sv = string_view((char*)d.data(), d.size());

                double res;

                auto [p, ec] = from_chars((char*)sv.data(), (char*)sv.data() + sv.size(), res);

                if (ec == errc::invalid_argument)
                    throw formatted_error("Cannot convert string \"{}\" to float", sv);
                else if (ec == errc::result_out_of_range)
                    throw formatted_error("String \"{}\" was too large to convert to float.", sv);

                return res;
            }

            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
            case sql_type::NTEXT:
            {
                if (d.empty())
                    return 0.0;

                u16string_view v((char16_t*)d.data(), d.size() / sizeof(char16_t));
                string s;

                s.reserve(v.length());

                for (auto c : v) {
                    s += (char)c;
                }

                double res;

                auto [p, ec] = from_chars(s.data(), s.data() + s.length(), res);

                if (ec == errc::invalid_argument)
                    throw formatted_error("Cannot convert string \"{}\" to float", s);
                else if (ec == errc::result_out_of_range)
                    throw formatted_error("String \"{}\" was too large to convert to float.", s);

                return res;
            }

            case sql_type::DATETIME: {
                auto dt = *(int32_t*)d.data();
                auto t = *(uint32_t*)(d.data() + sizeof(int32_t));

                return (double)dt + ((double)t / 25920000.0);
            }

            case sql_type::DATETIME2: {
                uint32_t n = 0;
                uint64_t secs = 0;

                memcpy(&n, d.data() + d.size() - 3, 3);

                memcpy(&secs, d.data(), min(sizeof(uint64_t), d.size() - 3));

                for (auto n = max_length2; n > 0; n--) {
                    secs /= 10;
                }

                return (double)(n - jan1900) + ((double)secs / 86400.0);
            }

            case sql_type::DATETIMEOFFSET: {
                uint32_t n = 0;
                uint64_t secs = 0;

                memcpy(&n, d.data() + d.size() - 5, 3);

                memcpy(&secs, d.data(), min(sizeof(uint64_t), d.size() - 5));

                for (auto n = max_length2; n > 0; n--) {
                    secs /= 10;
                }

                return (double)(n - jan1900) + ((double)secs / 86400.0);
            }

            case sql_type::DATETIMN:
                switch (d.size()) {
                    case 4: {
                        auto dt = *(uint16_t*)d.data();
                        auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));

                        return (double)dt + ((double)t / 1440.0);
                    }

                    case 8: {
                        auto dt = *(int32_t*)d.data();
                        auto t = *(uint32_t*)(d.data() + sizeof(int32_t));

                        return (double)dt + ((double)t / 25920000.0);
                    }

                    default:
                        throw formatted_error("DATETIMN has invalid length {}", d.size());
                }

            case sql_type::DATETIM4: {
                auto dt = *(uint16_t*)d.data();
                auto t = *(uint16_t*)(d.data() + sizeof(uint16_t));

                return (double)dt + ((double)t / 1440.0);
            }

            case sql_type::NUMERIC:
            case sql_type::DECIMAL: {
                auto s = (string)*this;

                try {
                    return stod(s);
                } catch (...) {
                    throw formatted_error("Cannot convert {} to float", s);
                }
            }

            case sql_type::MONEYN:
                switch (d.size()) {
                    case sizeof(int64_t): {
                        auto v = *(int64_t*)d.data();

                        v = (v >> 32) | ((v & 0xffffffff) << 32);

                        return (double)v / 10000.0;
                    }

                    case sizeof(int32_t): {
                        auto v = *(int32_t*)d.data();

                        return (double)v / 10000.0;
                    }

                    default:
                        throw formatted_error("MONEYN has unexpected length {}", d.size());
                }

            case sql_type::MONEY: {
                auto v = *(int64_t*)d.data();

                v = (v >> 32) | ((v & 0xffffffff) << 32);

                return (double)v / 10000.0;
            }

            case sql_type::SMALLMONEY:  {
                auto v = *(int32_t*)d.data();

                return (double)v / 10000.0;
            }

            // MSSQL doesn't allow conversion to FLOAT for DATE, TIME, DATETIME2, DATETIMEOFFSET, or VARBINARY

            default:
                throw formatted_error("Cannot convert {} to float", type2);
        }
    }

    static string quote_string(string_view s) {
        string ret;

        ret.reserve(s.size() + 2);

        ret += "'";

        for (auto c : s) {
            if (c == '\'')
                ret += "''";
            else
                ret += c;
        }

        ret += "'";

        return ret;
    }

    string value::to_literal() const {
        if (is_null)
            return "NULL";

        unsigned int max_length2 = max_length;
        auto type2 = type;
        span d = val;

        if (type == sql_type::SQL_VARIANT) {
            type2 = (sql_type)d[0];

            d = d.subspan(1);

            auto propbytes = d[0];

            d = d.subspan(1);

            switch (type2) {
                case sql_type::TIME:
                case sql_type::DATETIME2:
                case sql_type::DATETIMEOFFSET:
                    max_length2 = d[0];
                    break;

                default:
                    break;
            }

            d = d.subspan(propbytes);
        }

        switch (type2) {
            case sql_type::INTN:
            case sql_type::TINYINT:
            case sql_type::SMALLINT:
            case sql_type::INT:
            case sql_type::BIGINT:
            case sql_type::BIT:
            case sql_type::BITN:
                return to_string((int64_t)*this);

            case sql_type::TEXT:
            case sql_type::VARCHAR:
            case sql_type::CHAR:
            case sql_type::XML:
            case sql_type::UNIQUEIDENTIFIER:
                return quote_string((string)*this);

            case sql_type::NTEXT:
            case sql_type::NVARCHAR:
            case sql_type::NCHAR:
                return "N" + quote_string((string)*this);

            case sql_type::IMAGE:
            case sql_type::VARBINARY:
            case sql_type::BINARY:
            case sql_type::UDT: {
                string ret = "0x";

                for (auto b : d) {
                    ret += format("{:02x}", b);
                }

                return ret;
            }

            case sql_type::FLOAT:
            case sql_type::REAL:
            case sql_type::FLTN:
                return format("{}", (double)*this);

            case sql_type::MONEY:
            case sql_type::SMALLMONEY:
            case sql_type::MONEYN:
            case sql_type::DECIMAL:
            case sql_type::NUMERIC:
                return (string)*this;

            case sql_type::DATE: {
                auto ymd = (chrono::year_month_day)*this;

                return format("'{:04}{:02}{:02}'", (int)ymd.year(), (unsigned int)ymd.month(), (unsigned int)ymd.day());
            }

            case sql_type::TIME:
                return "'" + (string)*this + "'";

            case sql_type::DATETIME: {
                auto dt = (datetime)*this;
                chrono::hh_mm_ss hms(dt.t);
                constexpr decltype(dt.t)::period ratio;
                constexpr double ratio2 = (double)ratio.num / (double)ratio.den;

                double s = (double)hms.seconds().count() + ((double)hms.subseconds().count() * ratio2);

                return format("'{:04}{:02}{:02} {:02}:{:02}:{:06.3f}'", (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                                                                        hms.hours().count(), hms.minutes().count(), s);
            }

            case sql_type::DATETIMN: {
                auto dt = (datetime)*this;
                chrono::hh_mm_ss hms(dt.t);

                switch (d.size()) {
                    case 4:
                        return format("'{:04}{:02}{:02} {:02}:{:02}'", (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                                                                            hms.hours().count(), hms.minutes().count());

                    case 8: {
                        constexpr decltype(dt.t)::period ratio;
                        constexpr double ratio2 = (double)ratio.num / (double)ratio.den;
                        double s = (double)hms.seconds().count() + ((double)hms.subseconds().count() * ratio2);

                        return format("'{:04}{:02}{:02} {:02}:{:02}:{:06.3f}'", (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                                                                                hms.hours().count(), hms.minutes().count(), s);
                    }

                    default:
                        throw formatted_error("DATETIMN has invalid length {}.", d.size());
                }
            }

            case sql_type::DATETIM4: {
                auto dt = (datetime)*this;
                chrono::hh_mm_ss hms(dt.t);

                return format("'{:04}{:02}{:02} {:02}:{:02}'", (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                                                                    hms.hours().count(), hms.minutes().count());
            }

            case sql_type::DATETIME2: {
                auto dt = (datetime)*this;
                chrono::hh_mm_ss hms(dt.t);

                if (max_length2 == 0) {
                    return format("'{:04}{:02}{:02} {:02}:{:02}:{:02}'", (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                                    hms.hours().count(), hms.minutes().count(), hms.seconds().count());
                } else {
                    constexpr decltype(dt.t)::period ratio;
                    constexpr double ratio2 = (double)ratio.num / (double)ratio.den;
                    double s = (double)hms.seconds().count() + ((double)hms.subseconds().count() * ratio2);

                    return format("'{:04}{:02}{:02} {:02}:{:02}:{:0{}.{}f}'", (int)dt.d.year(), (unsigned int)dt.d.month(), (unsigned int)dt.d.day(),
                                    hms.hours().count(), hms.minutes().count(), s, max_length2 + 3, max_length2);
                }
            }

            case sql_type::DATETIMEOFFSET: {
                auto dto = (datetimeoffset)*this;

                auto d = dto.d;
                auto t = dto.t;

                t += chrono::minutes{dto.offset};

                if (t < time_t::zero()) {
                    d = chrono::year_month_day{(chrono::sys_days)d - chrono::days{1}};
                    t += chrono::days{1};
                } else if (t >= chrono::days{1}) {
                    d = chrono::year_month_day{(chrono::sys_days)d + chrono::days{1}};
                    t -= chrono::days{1};
                }

                chrono::hh_mm_ss hms(t);

                if (max_length2 == 0) {
                    return format("'{:04}{:02}{:02} {:02}:{:02}:{:02}{:+03}:{:02}'",
                                        (int)dto.d.year(), (unsigned int)dto.d.month(), (unsigned int)dto.d.day(),
                                        hms.hours().count(), hms.minutes().count(), hms.seconds().count(),
                                        dto.offset / 60, abs(dto.offset) % 60);
                } else {
                    constexpr decltype(t)::period ratio;
                    constexpr double ratio2 = (double)ratio.num / (double)ratio.den;
                    double s = (double)hms.seconds().count() + ((double)hms.subseconds().count() * ratio2);

                    return format("'{:04}{:02}{:02} {:02}:{:02}:{:0{}.{}f}{:+03}:{:02}'",
                                    (int)dto.d.year(), (unsigned int)dto.d.month(), (unsigned int)dto.d.day(),
                                    hms.hours().count(), hms.minutes().count(), s, max_length2 + 3, max_length2,
                                    dto.offset / 60, abs(dto.offset) % 60);
                }
            }

            default:
                throw formatted_error("Cannot convert {} to literal.", type2);
        }
    }
};
