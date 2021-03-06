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

use Avro\Schema\AvroPrimitiveSchema;
use Avro\Schema\AvroSchema;
use Avro\Exception\AvroSchemaParseException;

require_once('test_helper.php');

class SchemaExample {
  var $schemaString;
  var $isValid;
  var $name;
  var $comment;
  var $normalizedSchemaString;

  function __construct($schemaString, $isValid, $normalizedSchemaString = null, $name = null, $comment = null) {
    $this->schemaString = $schemaString;
    $this->isValid = $isValid;
    $this->name = $name ? $name : $schemaString;
    $this->normalizedSchemaString = $normalizedSchemaString ? $normalizedSchemaString : json_encode(json_decode($schemaString, true));
    $this->comment = $comment;
  }
}

class SchemaTest extends PHPUnit\Framework\TestCase {

  static $examples = [];
  static $validExamples = [];

  protected static function makePrimitiveExamples() {
    AvroPrimitiveSchema::setJavaStringType(false);

    $examples = [];
    foreach ([
               'null',
               'boolean',
               'int',
               'long',
               'float',
               'double',
               'bytes',
               'string'
             ]
             as $type) {
      $examples [] = new SchemaExample(sprintf('"%s"', $type), true);
      $examples [] = new SchemaExample(sprintf('{"type": "%s"}', $type), true, sprintf('"%s"', $type));
    }
    return $examples;
  }

  protected static function makeExamples() {
    $primitiveExamples = array_merge([
      new SchemaExample('"True"', false),
      new SchemaExample('{"no_type": "test"}', false),
      new SchemaExample('{"type": "panther"}', false)
    ],
      self::makePrimitiveExamples());

    $arrayExamples = [
      new SchemaExample('{"type": "array", "items": "long"}', true),
      new SchemaExample('
    {"type": "array",
     "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    ', true)
    ];

    $mapExamples = [
      new SchemaExample('{"type": "map", "values": "long"}', true),
      new SchemaExample('
    {"type": "map",
     "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    ', true)
    ];

    $unionExamples = [
      new SchemaExample('["string", "null", "long"]', true),
      new SchemaExample('["null", "null"]', false),
      new SchemaExample('["long", "long"]', false),
      new SchemaExample('
    [{"type": "array", "items": "long"}
     {"type": "array", "items": "string"}]
    ', false),
      new SchemaExample('["long",
                          {"type": "long"},
                          "int"]', false),
      new SchemaExample('["long",
                          {"type": "array", "items": "long"},
                          {"type": "map", "values": "long"},
                          "int"]', true),
      new SchemaExample('["long",
                          ["string", "null"],
                          "int"]', false),
      new SchemaExample('["long",
                          ["string", "null"],
                          "int"]', false),
      new SchemaExample('["null", "boolean", "int", "long", "float", "double",
                          "string", "bytes",
                          {"type": "array", "items":"int"},
                          {"type": "map", "values":"int"},
                          {"name": "bar", "type":"record",
                           "fields":[{"name":"label", "type":"string"}]},
                          {"name": "foo", "type":"fixed",
                           "size":16},
                          {"name": "baz", "type":"enum", "symbols":["A", "B", "C"]}
                         ]', true, '["null","boolean","int","long","float","double","string","bytes",{"type":"array","items":"int"},{"type":"map","values":"int"},{"type":"record","name":"bar","fields":[{"name":"label","type":"string"}]},{"type":"fixed","name":"foo","size":16},{"type":"enum","name":"baz","symbols":["A","B","C"]}]'),
      new SchemaExample('
    [{"name":"subtract", "namespace":"com.example",
      "type":"record",
      "fields":[{"name":"minuend", "type":"int"},
                {"name":"subtrahend", "type":"int"}]},
      {"name": "divide", "namespace":"com.example",
      "type":"record",
      "fields":[{"name":"quotient", "type":"int"},
                {"name":"dividend", "type":"int"}]},
      {"type": "array", "items": "string"}]
    ', true, '[{"type":"record","name":"subtract","namespace":"com.example","fields":[{"name":"minuend","type":"int"},{"name":"subtrahend","type":"int"}]},{"type":"record","name":"divide","namespace":"com.example","fields":[{"name":"quotient","type":"int"},{"name":"dividend","type":"int"}]},{"type":"array","items":"string"}]'),
    ];

    $fixedExamples = [
      new SchemaExample('{"type": "fixed", "name": "Test", "size": 1}', true),
      new SchemaExample('
    {"type": "fixed",
     "name": "MyFixed",
     "namespace": "org.apache.hadoop.avro",
     "size": 1}
    ', true),
      new SchemaExample('
    {"type": "fixed",
     "name": "Missing size"}
    ', false),
      new SchemaExample('
    {"type": "fixed",
     "size": 314}
    ', false),
      new SchemaExample('{"type":"fixed","name":"ex","doc":"this should be ignored","size": 314}',
        true,
        '{"type":"fixed","name":"ex","size":314}'),
      new SchemaExample('{"name": "bar",
                          "namespace": "com.example",
                          "type": "fixed",
                          "size": 32 }', true,
        '{"type":"fixed","name":"bar","namespace":"com.example","size":32}'),
      new SchemaExample('{"name": "com.example.bar",
                          "type": "fixed",
                          "size": 32 }', true,
        '{"type":"fixed","name":"bar","namespace":"com.example","size":32}')
    ];

    $recordExamples[] = new SchemaExample(
      '{"type":"fixed","name":"_x.bar","size":4}', true,
      '{"type":"fixed","name":"bar","namespace":"_x","size":4}');
    $recordExamples[] = new SchemaExample(
      '{"type":"fixed","name":"baz._x","size":4}', true,
      '{"type":"fixed","name":"_x","namespace":"baz","size":4}');
    $recordExamples[] = new SchemaExample(
      '{"type":"fixed","name":"baz.3x","size":4}', false);

    $enumExamples = [
      new SchemaExample('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', true),
      new SchemaExample('
    {"type": "enum",
     "name": "Status",
     "symbols": "Normal Caution Critical"}
    ', false),
      new SchemaExample('
    {"type": "enum",
     "name": [ 0, 1, 1, 2, 3, 5, 8 ],
     "symbols": ["Golden", "Mean"]}
    ', false),
      new SchemaExample('
    {"type": "enum",
     "symbols" : ["I", "will", "fail", "no", "name"]}
    ', false),
      new SchemaExample('
    {"type": "enum",
     "name": "Test"
     "symbols" : ["AA", "AA"]}
    ', false),
      new SchemaExample('{"type":"enum","name":"Test","symbols":["AA", 16]}',
        false),
      new SchemaExample('
    {"type": "enum",
     "name": "blood_types",
     "doc": "AB is freaky.",
     "symbols" : ["A", "AB", "B", "O"]}
    ', true),
      new SchemaExample('
    {"type": "enum",
     "name": "blood-types",
     "doc": 16,
     "symbols" : ["A", "AB", "B", "O"]}
    ', false)
    ];

    $recordExamples = [];
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "error",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Node",
     "fields": [{"name": "label", "type": "string"},
                {"name": "children",
                 "type": {"type": "array", "items": "Node"}}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "ListLink",
     "fields": [{"name": "car", "type": "int"},
                {"name": "cdr", "type": "ListLink"}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string"]}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "string"},
                                      {"name": "cdr", "type": "string"}]}]}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "Lisp"},
                                      {"name": "cdr", "type": "Lisp"}]}]}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "HandshakeResponse",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "match",
                 "type": {"type": "enum",
                          "name": "HandshakeMatch",
                          "symbols": ["BOTH", "CLIENT", "NONE"]}},
                {"name": "serverProtocol", "type": ["null", "string"]},
                {"name": "serverHash",
                 "type": ["null",
                          {"name": "MD5", "size": 16, "type": "fixed"}]},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true,
      '{"type":"record","name":"HandshakeResponse","namespace":"org.apache.avro.ipc","fields":[{"name":"match","type":{"type":"enum","name":"HandshakeMatch","symbols":["BOTH","CLIENT","NONE"]}},{"name":"serverProtocol","type":["null","string"]},{"name":"serverHash","type":["null",{"type":"fixed","name":"MD5","size":16}]},{"name":"meta","type":["null",{"type":"map","values":"bytes"}]}]}'
    );
    $recordExamples[] = new SchemaExample('{"type": "record",
 "namespace": "org.apache.avro",
 "name": "Interop",
 "fields": [{"type": {"fields": [{"type": {"items": "org.apache.avro.Node",
                                           "type": "array"},
                                  "name": "children"}],
                      "type": "record",
                      "name": "Node"},
             "name": "recordField"}]}
', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
    $recordExamples[] = new SchemaExample('{"type": "record",
 "namespace": "org.apache.avro",
 "name": "Interop",
 "fields": [{"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"},
             "name": "enumField"},
            {"type": {"fields": [{"type": "string", "name": "label"},
                                 {"type": {"items": "org.apache.avro.Node", "type": "array"},
                                  "name": "children"}],
                      "type": "record",
                      "name": "Node"},
             "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');

    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Interop",
     "namespace": "org.apache.avro",
     "fields": [{"name": "intField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "bytesField", "type": "bytes"},
                {"name": "nullField", "type": "null"},
                {"name": "arrayField",
                 "type": {"type": "array", "items": "double"}},
                {"name": "mapField",
                 "type": {"type": "map",
                          "values": {"name": "Foo",
                                     "type": "record",
                                     "fields": [{"name": "label",
                                                 "type": "string"}]}}},
                {"name": "unionField",
                 "type": ["boolean",
                          "double",
                          {"type": "array", "items": "bytes"}]},
                {"name": "enumField",
                 "type": {"type": "enum",
                          "name": "Kind",
                          "symbols": ["A", "B", "C"]}},
                {"name": "fixedField",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "recordField",
                 "type": {"type": "record",
                          "name": "Node",
                          "fields": [{"name": "label", "type": "string"},
                                     {"name": "children",
                                      "type": {"type": "array",
                                               "items": "Node"}}]}}]}
    ', true,
      '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":"string"},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":"string"}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
    $recordExamples[] = new SchemaExample('{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": "int", "name": "intField"}, {"type": "long", "name": "longField"}, {"type": "string", "name": "stringField"}, {"type": "boolean", "name": "boolField"}, {"type": "float", "name": "floatField"}, {"type": "double", "name": "doubleField"}, {"type": "bytes", "name": "bytesField"}, {"type": "null", "name": "nullField"}, {"type": {"items": "double", "type": "array"}, "name": "arrayField"}, {"type": {"type": "map", "values": {"fields": [{"type": "string", "name": "label"}], "type": "record", "name": "Foo"}}, "name": "mapField"}, {"type": ["boolean", "double", {"items": "bytes", "type": "array"}], "name": "unionField"}, {"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"}, "name": "enumField"}, {"type": {"type": "fixed", "name": "MD5", "size": 16}, "name": "fixedField"}, {"type": {"fields": [{"type": "string", "name": "label"}, {"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}
', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":"string"},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":"string"}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "ipAddr",
     "fields": [{"name": "addr",
                 "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}
    ', true,
      '{"type":"record","name":"ipAddr","fields":[{"name":"addr","type":[{"type":"fixed","name":"IPv6","size":16},{"type":"fixed","name":"IPv4","size":4}]}]}');
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Address",
     "fields": [{"type": "string"},
                {"type": "string", "name": "City"}]}
    ', false);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "name": "Event",
     "fields": [{"name": "Sponsor"},
                {"name": "City", "type": "string"}]}
    ', false);
    $recordExamples[] = new SchemaExample('
    {"type": "record",
     "fields": "His vision, from the constantly passing bars,"
     "name", "Rainer"}
    ', false);
    $recordExamples[] = new SchemaExample('
    {"name": ["Tom", "Jerry"],
     "type": "record",
     "fields": [{"name": "name", "type": "string"}]}
    ', false);
    $recordExamples[] = new SchemaExample('
    {"type":"record","name":"foo","doc":"doc string",
     "fields":[{"name":"bar", "type":"int", "order":"ascending", "default":1}]}
',
      true,
      '{"type":"record","name":"foo","doc":"doc string","fields":[{"name":"bar","type":"int","default":1,"order":"ascending"}]}');
    $recordExamples[] = new SchemaExample('
    {"type":"record", "name":"foo", "doc":"doc string",
     "fields":[{"name":"bar", "type":"int", "order":"bad"}]}
', false);

    self::$examples = array_merge($primitiveExamples,
      $fixedExamples,
      $enumExamples,
      $arrayExamples,
      $mapExamples,
      $unionExamples,
      $recordExamples);
    self::$validExamples = [];
    foreach (self::$examples as $example) {
      if ($example->isValid) {
        self::$validExamples[] = $example;
      }
    }
  }

  function testJsonDecode() {
    $this->assertEquals(json_decode('null', true), null);
    $this->assertEquals(json_decode('32', true), 32);
    $this->assertEquals(json_decode('"32"', true), '32');
    $this->assertEquals((array) json_decode('{"foo": 27}'), ["foo" => 27]);
    $this->assertTrue(is_array(json_decode('{"foo": 27}', true)));
    $this->assertEquals(json_decode('{"foo": 27}', true), ["foo" => 27]);
    $this->assertEquals(json_decode('["bar", "baz", "blurfl"]', true),
      ["bar", "baz", "blurfl"]);
    $this->assertFalse(is_array(json_decode('null', true)));
    $this->assertEquals(json_decode('{"type": "null"}', true), ["type" => 'null']);
    $this->assertEquals(json_decode('true', true), true);
    foreach (['True', 'TRUE', 'tRue'] as $truthy) {
      $this->assertNull(json_decode($truthy, true), $truthy);
    }
    $this->assertEquals(json_decode('"boolean"'), 'boolean');
  }

  function schemaExamplesProvider() {
    self::makeExamples();
    $ary = [];
    foreach (self::$examples as $example) {
      $ary[] = [$example];
    }
    return $ary;
  }

  /**
   * @dataProvider schemaExamplesProvider
   * @param $example
   */
  function testParse($example) {
    AvroPrimitiveSchema::setJavaStringType(false);
    $schemaString = $example->schemaString;
    try {
      $normalizedSchemaString = $example->normalizedSchemaString;
      $schema = AvroSchema::parse($schemaString);
      $this->assertTrue($example->isValid, sprintf("schemaString: %s\n", $schemaString));
      $this->assertEquals($normalizedSchemaString, strval($schema));
    } catch (AvroSchemaParseException $e) {
      $this->assertFalse($example->isValid, sprintf("schemaString: %s\n%s", $schemaString, $e->getMessage()));
    }
  }

}
