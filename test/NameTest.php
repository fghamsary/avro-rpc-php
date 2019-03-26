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

use Avro\Schema\AvroName;
use Avro\Exception\AvroSchemaParseException;

require_once('test_helper.php');

class NameExample {
  var $isValid;
  var $name;
  var $namespace;
  var $defaultNamespace;
  var $expectedFullname;

  function __construct($name, $namespace, $defaultNamespace, $isValid,
                       $expectedFullname = null) {
    $this->name = $name;
    $this->namespace = $namespace;
    $this->defaultNamespace = $defaultNamespace;
    $this->isValid = $isValid;
    $this->expectedFullname = $expectedFullname;
  }

  function __toString() {
    return var_export($this, true);
  }
}

class NameTest extends PHPUnit\Framework\TestCase {

  function fullnameProvider() {
    return [
      [new NameExample('foo', null, null, true, 'foo')],
      [new NameExample('foo', 'bar', null, true, 'bar.foo')],
      [new NameExample('bar.foo', 'baz', null, true, 'bar.foo')],
      [new NameExample('_bar.foo', 'baz', null, true, '_bar.foo')],
      [new NameExample('bar._foo', 'baz', null, true, 'bar._foo')],
      [new NameExample('3bar.foo', 'baz', null, false)],
      [new NameExample('bar.3foo', 'baz', null, false)],
      [new NameExample('b4r.foo', 'baz', null, true, 'b4r.foo')],
      [new NameExample('bar.f0o', 'baz', null, true, 'bar.f0o')],
      [new NameExample(' .foo', 'baz', null, false)],
      [new NameExample('bar. foo', 'baz', null, false)],
      [new NameExample('bar. ', 'baz', null, false)],
    ];
  }

  /**
   * @dataProvider fullnameProvider
   * @param NameExample $ex
   */
  function testFullname(NameExample $ex) {
    try {
      $name = new AvroName($ex->name, $ex->namespace, $ex->defaultNamespace);
      $this->assertTrue($ex->isValid);
      $this->assertEquals($ex->expectedFullname, $name->getFullname());
    } catch (AvroSchemaParseException $e) {
      $this->assertFalse($ex->isValid, sprintf("%s:\n%s",
        $ex,
        $e->getMessage()));
    }
  }

  function nameProvider() {
    return [
      ['a', true],
      ['_', true],
      ['1a', false],
      ['', false],
      [null, false],
      [' ', false],
      ['Cons', true]
    ];
  }

  /**
   * @dataProvider nameProvider
   * @param $name
   * @param $isWellFormed
   */
  function testName($name, $isWellFormed) {
    $this->assertEquals(AvroName::isWellFormedName($name), $isWellFormed, (string)$name);
  }
}
