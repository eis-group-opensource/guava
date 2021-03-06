/*
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.common.collect;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.GwtCompatible;
import com.google.common.annotations.GwtIncompatible;
import com.google.j2objc.annotations.Weak;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * {@code entrySet()} implementation for {@link ImmutableMap}.
 *
 * @author Jesse Wilson
 * @author Kevin Bourrillion
 */
@GwtCompatible(emulated = true)
abstract class ImmutableMapEntrySet<K, V> extends ImmutableSet<Entry<K, V>> {
  static final class RegularEntrySet<K, V> extends ImmutableMapEntrySet<K, V> {
    @Weak private final transient ImmutableMap<K, V> map;
    private final transient Entry<K, V>[] entries;

    RegularEntrySet(ImmutableMap<K, V> map, Entry<K, V>[] entries) {
      this.map = map;
      this.entries = entries;
    }

    @Override
    ImmutableMap<K, V> map() {
      return map;
    }

    @Override
    public UnmodifiableIterator<Entry<K, V>> iterator() {
      return Iterators.forArray(entries);
    }

    @Override
    public Spliterator<Entry<K, V>> spliterator() {
      return Spliterators.spliterator(entries, ImmutableSet.SPLITERATOR_CHARACTERISTICS);
    }

    @Override
    public void forEach(Consumer<? super Entry<K, V>> action) {
      checkNotNull(action);
      for (Entry<K, V> entry : entries) {
        action.accept(entry);
      }
    }

    @Override
    ImmutableList<Entry<K, V>> createAsList() {
      return new RegularImmutableAsList<>(this, entries);
    }
  }

  ImmutableMapEntrySet() {}

  abstract ImmutableMap<K, V> map();

  @Override
  public int size() {
    return map().size();
  }

  @Override
  public boolean contains(@NullableDecl Object object) {
    if (object instanceof Entry) {
      Entry<?, ?> entry = (Entry<?, ?>) object;
      V value = map().get(entry.getKey());
      return value != null && value.equals(entry.getValue());
    }
    return false;
  }

  @Override
  boolean isPartialView() {
    return map().isPartialView();
  }

  @Override
  @GwtIncompatible // not used in GWT
  boolean isHashCodeFast() {
    return map().isHashCodeFast();
  }

  @Override
  public int hashCode() {
    return map().hashCode();
  }

  @GwtIncompatible // serialization
  @Override
  Object writeReplace() {
    return new EntrySetSerializedForm<>(map());
  }

  @GwtIncompatible // serialization
  private static class EntrySetSerializedForm<K, V> implements Serializable {
    final ImmutableMap<K, V> map;

    EntrySetSerializedForm(ImmutableMap<K, V> map) {
      this.map = map;
    }

    Object readResolve() {
      return map.entrySet();
    }

    private static final long serialVersionUID = 0;
  }
}
