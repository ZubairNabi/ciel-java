package com.asgow.ciel.examples.mapreduce.terasort;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;

import com.asgow.ciel.examples.mapreduce.wordcount.Text;



/**
 * <code>RawKeyValueIterator</code> is an iterator used to iterate over
 * the raw keys and values during sort/merge of intermediate data. 
 */
public interface TextPairIterator {
  /** 
   * Gets the current raw key.
   * 
   * @return Gets the current raw key as a DataInputBuffer
   * @throws IOException
   */
  Text getKey() throws IOException;
  
  /** 
   * Gets the current raw value.
   * 
   * @return Gets the current raw value as a DataInputBuffer 
   * @throws IOException
   */
  Text getValue() throws IOException;
  
  /** 
   * Sets up the current key and value (for getKey and getValue).
   * 
   * @return <code>true</code> if there exists a key/value, 
   *         <code>false</code> otherwise. 
   * @throws IOException
   */
  boolean next() throws IOException;
  
  /** 
   * Closes the iterator so that the underlying streams can be closed.
   * 
   * @throws IOException
   */
  void close() throws IOException;
  
}
