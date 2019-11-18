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

require_once('DataFileTest.php');
require_once('SchemaTest.php');
require_once('SchemaJavaStringTest.php');
require_once('NameTest.php');
require_once('StringIOTest.php');
require_once('IODatumReaderTest.php');
require_once('LongEncodingTest.php');
require_once('FloatIntEncodingTest.php');
require_once('DatumIOTest.php');
require_once('ProtocolFileTest.php');
require_once('IpcTest.php');
require_once('AvroProtocolTest.php');
require_once('TestProtocol/ProtocolClasses.php');
// InterOpTest tests are run separately.

class AllTests {

  public static function suite() {
    $suite = new PHPUnit\Framework\TestSuite('AvroAllTests');
    $suite->addTestSuite(DataFileTest::class);
    $suite->addTestSuite(SchemaTest::class);
    $suite->addTestSuite(NameTest::class);
    $suite->addTestSuite(StringIOTest::class);
    $suite->addTestSuite(IODatumReaderTest::class);
    $suite->addTestSuite(LongEncodingTest::class);
    $suite->addTestSuite(FloatIntEncodingTest::class);
    $suite->addTestSuite(DatumIOTest::class);
    $suite->addTestSuite(ProtocolFileTest::class);
    $suite->addTestSuite(IpcTest::class);
    $suite->addTestSuite(SchemaJavaStringTest::class);
    $suite->addTestSuite(AvroProtocolTest::class);
    return $suite;
  }

}
