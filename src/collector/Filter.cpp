/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
   This file is part of Mastermind.

   Mastermind is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   Mastermind is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with Mastermind.
*/

#include "Filter.h"

#include <algorithm>

void Filter::sort()
{
    std::sort(namespaces.begin(), namespaces.end());
    std::sort(couples.begin(), couples.end());
    std::sort(groups.begin(), groups.end());
    std::sort(backends.begin(), backends.end());
    std::sort(nodes.begin(), nodes.end());
    std::sort(filesystems.begin(), filesystems.end());
}

bool Filter::empty() const
{
    return namespaces.empty() &&
        couples.empty() &&
        groups.empty() &&
        backends.empty() &&
        nodes.empty() &&
        filesystems.empty();
}