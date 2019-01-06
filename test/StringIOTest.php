<?php
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

use Avro\IO\Data\AvroDataIOReader;
use Avro\IO\Data\AvroDataIOWriter;
use Avro\AvroDebug;
use Avro\Schema\AvroSchema;
use Avro\IO\AvroStringIO;

require_once('test_helper.php');

class StringIOTest extends PHPUnit\Framework\TestCase {

  public function testWrite() {
    $strio = new AvroStringIO();
    $this->assertEquals(0, $strio->tell());
    $str = 'foo';
    $strlen = strlen($str);
    $this->assertEquals($strlen, $strio->write($str));
    $this->assertEquals($strlen, $strio->tell());
  }

  public function testSeek() {
    $this->markTestIncomplete('This test has not been implemented yet.');
  }

  public function testTell() {
    $this->markTestIncomplete('This test has not been implemented yet.');
  }

  public function testRead() {
    $this->markTestIncomplete('This test has not been implemented yet.');
  }

  public function testStringRep() {
    $writersSchemaJson = '"null"';
    $writersSchema = AvroSchema::parse($writersSchemaJson);
    $strio = new AvroStringIO();
    $this->assertEquals('', $strio->string());
    $dw = new AvroDataIOWriter($strio, $writersSchema);
    $dw->close();

    $this->assertEquals(57, strlen($strio->string()), AvroDebug::asciiString($strio->string()));

    $read_strio = new AvroStringIO($strio->string());

    $dr = new AvroDataIOReader($read_strio);
    $read_data = $dr->data();
    $datum_count = count($read_data);
    $this->assertEquals(0, $datum_count);
  }

}
