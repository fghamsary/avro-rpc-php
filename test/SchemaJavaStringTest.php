<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 07/01/2019
 * Time: 10:46
 */

use Avro\Exception\AvroSchemaParseException;
use Avro\Schema\AvroPrimitiveSchema;
use Avro\Schema\AvroSchema;

require_once('test_helper.php');

class SchemaStringExample {
  var $schemaString;
  var $isValid;
  var $name;
  var $comment;
  var $normalizedSchemaString;

  function __construct($schemaString, $is_valid, $normalizedSchemaString = null, $name = null, $comment = null) {
    $this->schemaString = $schemaString;
    $this->isValid = $is_valid;
    $this->name = $name ? $name : $schemaString;
    $this->normalizedSchemaString = $normalizedSchemaString ? $normalizedSchemaString : json_encode(json_decode($schemaString, true));
    $this->comment = $comment;
  }
}

class SchemaJavaStringTest extends PHPUnit\Framework\TestCase {

  static $examples = [];
  static $validExamples = [];

  protected static function makePrimitiveExamples() {

    $examples = [];
    foreach ([
               'null',
               'boolean',
               'int',
               'long',
               'float',
               'double',
               'bytes',
             ]
             as $type) {
      $examples[] = new SchemaStringExample(sprintf('"%s"', $type), true);
      $examples[] = new SchemaStringExample(sprintf('{"type": "%s"}', $type), true, sprintf('"%s"', $type));
    }
    $examples[] = new SchemaStringExample('"string"', true, '{"type":"string","avro.java.string":"String"}');
    $examples[] = new SchemaStringExample('{"type": "string"}', true, '{"type":"string","avro.java.string":"String"}');
    return $examples;
  }

  protected static function makeExamples() {
    $primitiveExamples = array_merge([
      new SchemaStringExample('"True"', false),
      new SchemaStringExample('{"no_type": "test"}', false),
      new SchemaStringExample('{"type": "panther"}', false)
    ],
      self::makePrimitiveExamples());

    $arrayExamples = [
      new SchemaStringExample('{"type": "array", "items": "long"}', true),
      new SchemaStringExample('
    {"type": "array",
     "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    ', true)
    ];

    $mapExamples = [
      new SchemaStringExample('{"type": "map", "values": "long"}', true),
      new SchemaStringExample('
    {"type": "map",
     "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    ', true)
    ];

    $unionExamples = [
      new SchemaStringExample('["string", "null", "long"]', true, '[{"type":"string","avro.java.string":"String"},"null","long"]'),
      new SchemaStringExample('["null", "null"]', false),
      new SchemaStringExample('["long", "long"]', false),
      new SchemaStringExample('
    [{"type": "array", "items": "long"}
     {"type": "array", "items": "string"}]
    ', false),
      new SchemaStringExample('["long",
                          {"type": "long"},
                          "int"]', false),
      new SchemaStringExample('["long",
                          {"type": "array", "items": "long"},
                          {"type": "map", "values": "long"},
                          "int"]', true),
      new SchemaStringExample('["long",
                          ["string", "null"],
                          "int"]', false),
      new SchemaStringExample('["long",
                          ["string", "null"],
                          "int"]', false),
      new SchemaStringExample('["null", "boolean", "int", "long", "float", "double",
                          "string", "bytes",
                          {"type": "array", "items":"int"},
                          {"type": "map", "values":"int"},
                          {"name": "bar", "type":"record",
                           "fields":[{"name":"label", "type":"string"}]},
                          {"name": "foo", "type":"fixed",
                           "size":16},
                          {"name": "baz", "type":"enum", "symbols":["A", "B", "C"]}
                         ]', true, '["null","boolean","int","long","float","double",{"type":"string","avro.java.string":"String"},"bytes",{"type":"array","items":"int"},{"type":"map","values":"int"},{"type":"record","name":"bar","fields":[{"name":"label","type":{"type":"string","avro.java.string":"String"}}]},{"type":"fixed","name":"foo","size":16},{"type":"enum","name":"baz","symbols":["A","B","C"]}]'),
      new SchemaStringExample('
    [{"name":"subtract", "namespace":"com.example",
      "type":"record",
      "fields":[{"name":"minuend", "type":"int"},
                {"name":"subtrahend", "type":"int"}]},
      {"name": "divide", "namespace":"com.example",
      "type":"record",
      "fields":[{"name":"quotient", "type":"int"},
                {"name":"dividend", "type":"int"}]},
      {"type": "array", "items": "string"}]
    ', true, '[{"type":"record","name":"subtract","namespace":"com.example","fields":[{"name":"minuend","type":"int"},{"name":"subtrahend","type":"int"}]},{"type":"record","name":"divide","namespace":"com.example","fields":[{"name":"quotient","type":"int"},{"name":"dividend","type":"int"}]},{"type":"array","items":{"type":"string","avro.java.string":"String"}}]'),
    ];

    $fixedExamples = [
      new SchemaStringExample('{"type": "fixed", "name": "Test", "size": 1}', true),
      new SchemaStringExample('
    {"type": "fixed",
     "name": "MyFixed",
     "namespace": "org.apache.hadoop.avro",
     "size": 1}
    ', true),
      new SchemaStringExample('
    {"type": "fixed",
     "name": "Missing size"}
    ', false),
      new SchemaStringExample('
    {"type": "fixed",
     "size": 314}
    ', false),
      new SchemaStringExample('{"type":"fixed","name":"ex","doc":"this should be ignored","size": 314}',
        true,
        '{"type":"fixed","name":"ex","size":314}'),
      new SchemaStringExample('{"name": "bar",
                          "namespace": "com.example",
                          "type": "fixed",
                          "size": 32 }', true,
        '{"type":"fixed","name":"bar","namespace":"com.example","size":32}'),
      new SchemaStringExample('{"name": "com.example.bar",
                          "type": "fixed",
                          "size": 32 }', true,
        '{"type":"fixed","name":"bar","namespace":"com.example","size":32}')
    ];

    $recordExamples[] = new SchemaStringExample(
      '{"type":"fixed","name":"_x.bar","size":4}', true,
      '{"type":"fixed","name":"bar","namespace":"_x","size":4}');
    $recordExamples[] = new SchemaStringExample(
      '{"type":"fixed","name":"baz._x","size":4}', true,
      '{"type":"fixed","name":"_x","namespace":"baz","size":4}');
    $recordExamples[] = new SchemaStringExample(
      '{"type":"fixed","name":"baz.3x","size":4}', false);

    $enumExamples = [
      new SchemaStringExample('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', true),
      new SchemaStringExample('
    {"type": "enum",
     "name": "Status",
     "symbols": "Normal Caution Critical"}
    ', false),
      new SchemaStringExample('
    {"type": "enum",
     "name": [ 0, 1, 1, 2, 3, 5, 8 ],
     "symbols": ["Golden", "Mean"]}
    ', false),
      new SchemaStringExample('
    {"type": "enum",
     "symbols" : ["I", "will", "fail", "no", "name"]}
    ', false),
      new SchemaStringExample('
    {"type": "enum",
     "name": "Test"
     "symbols" : ["AA", "AA"]}
    ', false),
      new SchemaStringExample('{"type":"enum","name":"Test","symbols":["AA", 16]}',
        false),
      new SchemaStringExample('
    {"type": "enum",
     "name": "blood_types",
     "doc": "AB is freaky.",
     "symbols" : ["A", "AB", "B", "O"]}
    ', true),
      new SchemaStringExample('
    {"type": "enum",
     "name": "blood-types",
     "doc": 16,
     "symbols" : ["A", "AB", "B", "O"]}
    ', false)
    ];

    $recordExamples = [];
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    ', true);
    $recordExamples[] = new SchemaStringExample('
    {"type": "error",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    ', true);
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "Node",
     "fields": [{"name": "label", "type": "string"},
                {"name": "children",
                 "type": {"type": "array", "items": "Node"}}]}
    ', true,'{"type":"record","name":"Node","fields":[{"name":"label","type":{"type":"string","avro.java.string":"String"}},{"name":"children","type":{"type":"array","items":"Node"}}]}');
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "ListLink",
     "fields": [{"name": "car", "type": "int"},
                {"name": "cdr", "type": "ListLink"}]}
    ', true);
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string"]}]}
    ', true,'{"type":"record","name":"Lisp","fields":[{"name":"value","type":["null",{"type":"string","avro.java.string":"String"}]}]}');
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "string"},
                                      {"name": "cdr", "type": "string"}]}]}]}
    ', true,'{"type":"record","name":"Lisp","fields":[{"name":"value","type":["null",{"type":"string","avro.java.string":"String"},{"type":"record","name":"Cons","fields":[{"name":"car","type":{"type":"string","avro.java.string":"String"}},{"name":"cdr","type":{"type":"string","avro.java.string":"String"}}]}]}]}');
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "Lisp"},
                                      {"name": "cdr", "type": "Lisp"}]}]}]}
    ', true,'{"type":"record","name":"Lisp","fields":[{"name":"value","type":["null",{"type":"string","avro.java.string":"String"},{"type":"record","name":"Cons","fields":[{"name":"car","type":"Lisp"},{"name":"cdr","type":"Lisp"}]}]}]}');
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true);
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true,'{"type":"record","name":"HandshakeRequest","namespace":"org.apache.avro.ipc","fields":[{"name":"clientHash","type":{"type":"fixed","name":"MD5","size":16}},{"name":"clientProtocol","type":["null",{"type":"string","avro.java.string":"String"}]},{"name":"serverHash","type":"MD5"},{"name":"meta","type":["null",{"type":"map","values":"bytes"}]}]}');
    $recordExamples[] = new SchemaStringExample('
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
      '{"type":"record","name":"HandshakeResponse","namespace":"org.apache.avro.ipc","fields":[{"name":"match","type":{"type":"enum","name":"HandshakeMatch","symbols":["BOTH","CLIENT","NONE"]}},{"name":"serverProtocol","type":["null",{"type":"string","avro.java.string":"String"}]},{"name":"serverHash","type":["null",{"type":"fixed","name":"MD5","size":16}]},{"name":"meta","type":["null",{"type":"map","values":"bytes"}]}]}'
    );
    $recordExamples[] = new SchemaStringExample('{"type": "record",
 "namespace": "org.apache.avro",
 "name": "Interop",
 "fields": [{"type": {"fields": [{"type": {"items": "org.apache.avro.Node",
                                           "type": "array"},
                                  "name": "children"}],
                      "type": "record",
                      "name": "Node"},
             "name": "recordField"}]}
', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
    $recordExamples[] = new SchemaStringExample('{"type": "record",
 "namespace": "org.apache.avro",
 "name": "Interop",
 "fields": [{"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"},
             "name": "enumField"},
            {"type": {"fields": [{"type": "string", "name": "label"},
                                 {"type": {"items": "org.apache.avro.Node", "type": "array"},
                                  "name": "children"}],
                      "type": "record",
                      "name": "Node"},
             "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":{"type":"string","avro.java.string":"String"}},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');

    $recordExamples[] = new SchemaStringExample('
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
      '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":{"type":"string","avro.java.string":"String"}},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":{"type":"string","avro.java.string":"String"}}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":{"type":"string","avro.java.string":"String"}},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
    $recordExamples[] = new SchemaStringExample('{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": "int", "name": "intField"}, {"type": "long", "name": "longField"}, {"type": "string", "name": "stringField"}, {"type": "boolean", "name": "boolField"}, {"type": "float", "name": "floatField"}, {"type": "double", "name": "doubleField"}, {"type": "bytes", "name": "bytesField"}, {"type": "null", "name": "nullField"}, {"type": {"items": "double", "type": "array"}, "name": "arrayField"}, {"type": {"type": "map", "values": {"fields": [{"type": "string", "name": "label"}], "type": "record", "name": "Foo"}}, "name": "mapField"}, {"type": ["boolean", "double", {"items": "bytes", "type": "array"}], "name": "unionField"}, {"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"}, "name": "enumField"}, {"type": {"type": "fixed", "name": "MD5", "size": 16}, "name": "fixedField"}, {"type": {"fields": [{"type": "string", "name": "label"}, {"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}
', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":{"type":"string","avro.java.string":"String"}},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":{"type":"string","avro.java.string":"String"}}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":{"type":"string","avro.java.string":"String"}},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "ipAddr",
     "fields": [{"name": "addr",
                 "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}
    ', true,
      '{"type":"record","name":"ipAddr","fields":[{"name":"addr","type":[{"type":"fixed","name":"IPv6","size":16},{"type":"fixed","name":"IPv4","size":4}]}]}');
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "Address",
     "fields": [{"type": "string"},
                {"type": "string", "name": "City"}]}
    ', false);
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "name": "Event",
     "fields": [{"name": "Sponsor"},
                {"name": "City", "type": "string"}]}
    ', false);
    $recordExamples[] = new SchemaStringExample('
    {"type": "record",
     "fields": "His vision, from the constantly passing bars,"
     "name", "Rainer"}
    ', false);
    $recordExamples[] = new SchemaStringExample('
    {"name": ["Tom", "Jerry"],
     "type": "record",
     "fields": [{"name": "name", "type": "string"}]}
    ', false);
    $recordExamples[] = new SchemaStringExample('
    {"type":"record","name":"foo","doc":"doc string",
     "fields":[{"name":"bar", "type":"int", "order":"ascending", "default":1}]}
',
      true,
      '{"type":"record","name":"foo","doc":"doc string","fields":[{"name":"bar","type":"int","default":1,"order":"ascending"}]}');
    $recordExamples[] = new SchemaStringExample('
    {"type":"record", "name":"foo", "doc":"doc string",
     "fields":[{"name":"bar", "type":"int", "order":"bad"}]}
', false);

    $recordExamples[] = new SchemaStringExample('
      {
        "type": "record",
        "name": "foo",
        "fields": [
          {"name": "properties", "type": {"type":"map", "values": ["string", "null"]}}
        ]}
    ', true, '{"type":"record","name":"foo","fields":[{"name":"properties","type":{"type":"map","values":[{"type":"string","avro.java.string":"String"},"null"],"avro.java.string":"String"}}]}');

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
   */
  function testParse($example) {
    AvroPrimitiveSchema::setJavaStringType(true);
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