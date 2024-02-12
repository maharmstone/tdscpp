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

#include "ringbuf.h"
#include <stdlib.h>
#include <string.h>
#include <new>

using namespace std;

ringbuf::ringbuf(size_t length) : length(length) {
    data = (uint8_t*)malloc(length);
    if (!data)
        throw bad_alloc();
}

ringbuf::~ringbuf() {
    free(data);
}

void ringbuf::peek(span<uint8_t> sp) {
    size_t to_copy = min(sp.size(), length - offset);

    memcpy(sp.data(), data + offset, to_copy);

    if (sp.size() == to_copy)
        return;

    sp = sp.subspan(to_copy);

    memcpy(sp.data(), data, sp.size());
}

void ringbuf::discard(size_t bytes) {
    offset += bytes;
    offset %= length;
    used -= bytes;
}

void ringbuf::read(span<uint8_t> sp) {
    peek(sp);
    discard(sp.size());
}

void ringbuf::write(span<const uint8_t> sp) {
    if (offset + used < length) {
        size_t to_copy = min(sp.size(), length - offset - used);

        memcpy(data + offset + used, sp.data(), to_copy);
        used += to_copy;

        if (sp.size() == to_copy)
            return;

        sp = sp.subspan(to_copy);
    }

    memcpy(data + offset + used - length, sp.data(), sp.size());
    used += sp.size();
}

size_t ringbuf::size() const {
    return used;
}

size_t ringbuf::available() const {
    return length - used;
}

bool ringbuf::empty() const {
    return used == 0;
}

void ringbuf::clear() {
    used = 0;
}
