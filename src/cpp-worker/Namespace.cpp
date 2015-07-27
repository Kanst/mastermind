/*
 * Copyright (c) YANDEX LLC, 2015. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */

#include "Guard.h"
#include "Namespace.h"

void Namespace::add_couple(Couple *couple)
{
    WriteGuard<RWSpinLock> guard(m_couples_lock);
    m_couples.insert(couple);
}

size_t Namespace::get_couple_count() const
{
    ReadGuard<RWSpinLock> guard(m_couples_lock);
    return m_couples.size();
}

void Namespace::get_couples(std::vector<Couple*> & couples) const
{
    ReadGuard<RWSpinLock> guard(m_couples_lock);
    couples.assign(m_couples.begin(), m_couples.end());
}
