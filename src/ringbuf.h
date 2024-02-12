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

#include <stdint.h>
#include <stddef.h>
#include <span>

class ringbuf {
public:
    ringbuf(size_t length);
    ~ringbuf();
    void read(std::span<uint8_t> sp);
    void peek(std::span<uint8_t> sp);
    void write(std::span<const uint8_t> sp);
    void discard(size_t bytes);
    size_t size() const;
    size_t available() const;
    [[nodiscard]] bool empty() const;
    void clear();

private:
    uint8_t* data;
    size_t length;
    size_t offset = 0;
    size_t used = 0;
};
