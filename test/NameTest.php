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
  var $is_valid;
  var $name;
  var $namespace;
  var $default_namespace;
  var $expected_fullname;
  function __construct($name, $namespace, $default_namespace, $is_valid,
                       $expected_fullname = null) {
    $this->name = $name;
    $this->namespace = $namespace;
    $this->default_namespace = $default_namespace;
    $this->is_valid = $is_valid;
    $this->expected_fullname = $expected_fullname;
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
   */
  function testFullname(NameExample $ex) {
    try
    {
      $name = new AvroName($ex->name, $ex->namespace, $ex->default_namespace);
      $this->assertTrue($ex->is_valid);
      $this->assertEquals($ex->expected_fullname, $name->getFullname());
    }
    catch (AvroSchemaParseException $e)
    {
      $this->assertFalse($ex->is_valid, sprintf("%s:\n%s",
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
   */
  function testName($name, $is_well_formed) {
    $this->assertEquals(AvroName::isWellFormedName($name), $is_well_formed, (string)$name);
  }
}
