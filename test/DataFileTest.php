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

use Avro\IO\Data\AvroDataIO;

require_once('test_helper.php');

class DataFileTest extends PHPUnit\Framework\TestCase {

  private $dataFiles = [];
  const REMOVE_DATA_FILES = true;

  static function currentTimestamp() {
    return strftime("%Y%m%dT%H%M%S");
  }

  protected function addDataFile($dataFile) {
    $dataFile = "$dataFile." . self::currentTimestamp();
    $full = join(DIRECTORY_SEPARATOR, [TEST_TEMP_DIR, $dataFile]);
    $this->dataFiles[] = $full;
    return $full;
  }

  protected static function removeDataFile($dataFile) {
    if (file_exists($dataFile)) {
      unlink($dataFile);
    }
  }

  protected function removeDataFiles() {
    if (self::REMOVE_DATA_FILES
      && 0 < count($this->dataFiles)) {
      foreach ($this->dataFiles as $dataFile) {
        $this->removeDataFile($dataFile);
      }
    }
  }

  protected function setUp() {
    if (!file_exists(TEST_TEMP_DIR)) {
      mkdir(TEST_TEMP_DIR);
    }
    $this->removeDataFiles();
  }

  protected function tearDown() {
    $this->removeDataFiles();
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testWriteReadNothingRoundTrip() {
    $dataFile = $this->addDataFile('data-wr-nothing-null.avr');
    $writersSchema = '"null"';
    $dw = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
    $dw->close();

    $dr = AvroDataIO::openFile($dataFile);
    $data = $dr->data();
    $readData = array_shift($data);
    $dr->close();
    $this->assertEquals(null, $readData);
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testWriteReadNullRoundTrip() {
    $data_file = $this->addDataFile('data-wr-null.avr');
    $writers_schema = '"null"';
    $data = null;
    $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema);
    $dw->append($data);
    $dw->close();

    $dr = AvroDataIO::openFile($data_file);
    $dr_data = $dr->data();
    $read_data = array_shift($dr_data);
    $dr->close();
    $this->assertEquals($data, $read_data);
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testWriteReadStringRoundTrip() {
    $data_file = $this->addDataFile('data-wr-str.avr');
    $writers_schema = '"string"';
    $data = 'foo';
    $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema);
    $dw->append($data);
    $dw->close();

    $dr = AvroDataIO::openFile($data_file);
    $dr_data = $dr->data();
    $read_data = array_shift($dr_data);
    $dr->close();
    $this->assertEquals($data, $read_data);
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testWriteReadRoundTrip() {
    $data_file = $this->addDataFile('data-wr-int.avr');
    $writers_schema = '"int"';
    $data = 1;

    $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema);
    $dw->append(1);
    $dw->close();

    $dr = AvroDataIO::openFile($data_file);
    $dr_data = $dr->data();
    $read_data = array_shift($dr_data);
    $dr->close();
    $this->assertEquals($data, $read_data);
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testWriteReadTrueRoundTrip() {
    $data_file = $this->addDataFile('data-wr-true.avr');
    $writers_schema = '"boolean"';
    $datum = true;
    $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema);
    $dw->append($datum);
    $dw->close();

    $dr = AvroDataIO::openFile($data_file);
    $dr_data = $dr->data();
    $read_datum = array_shift($dr_data);
    $dr->close();
    $this->assertEquals($datum, $read_datum);
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testWriteReadFalseRoundTrip() {
    $data_file = $this->addDataFile('data-wr-false.avr');
    $writers_schema = '"boolean"';
    $datum = false;
    $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema);
    $dw->append($datum);
    $dw->close();

    $dr = AvroDataIO::openFile($data_file);
    $dr_data = $dr->data();
    $read_datum = array_shift($dr_data);
    $dr->close();
    $this->assertEquals($datum, $read_datum);
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testWriteReadIntArrayRoundTrip() {
    $data_file = $this->addDataFile('data-wr-int-ary.avr');
    $writers_schema = '"int"';
    $data = [10, 20, 30, 40, 50, 60, 70];
    $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema);
    foreach ($data as $datum) {
      $dw->append($datum);
    }
    $dw->close();

    $dr = AvroDataIO::openFile($data_file);
    $read_data = $dr->data();
    $dr->close();
    $this->assertEquals($data, $read_data, sprintf("in: %s\nout: %s", json_encode($data), json_encode($read_data)));
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testDifferingSchemasWithPrimitives() {
    $dataFile = $this->addDataFile('data-prim.avr');

    $writerSchema = <<<JSON
{ "type": "record",
  "name": "User",
  "fields" : [
      {"name": "username", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "verified", "type": "boolean", "default": "false"}
      ]}
JSON;
    $data = [
      ['username' => 'john', 'age' => 25, 'verified' => true],
      ['username' => 'ryan', 'age' => 23, 'verified' => false]
    ];
    $dw = AvroDataIO::openFile($dataFile, 'w', $writerSchema);
    foreach ($data as $datum) {
      $dw->append($datum);
    }
    $dw->close();
    $readerSchema = <<<JSON
      { "type": "record",
        "name": "User",
        "fields" : [
      {"name": "username", "type": "string"}
      ]}
JSON;
    $dr = AvroDataIO::openFile($dataFile, 'r', $readerSchema);
    $dr_data = $dr->data();
    foreach ($dr_data as $index => $record) {
      $this->assertEquals($data[$index]['username'], $record['username']);
    }
  }

  /**
   * @throws \Avro\Exception\AvroException
   * @throws \Avro\Exception\AvroSchemaParseException
   * @throws \Avro\IO\AvroIOSchemaMatchException
   * @throws \Avro\IO\AvroIOTypeException
   * @throws \Avro\IO\Exception\AvroDataIOException
   * @throws \Avro\IO\Exception\AvroIOException
   */
  public function testDifferingSchemasWithComplexObjects() {
    $dataFile = $this->addDataFile('data-complex.avr');

    $writersSchema = <<<JSON
{ "type": "record",
  "name": "something",
  "fields": [
    {"name": "something_fixed", "type": {"name": "inner_fixed",
                                         "type": "fixed", "size": 3}},
    {"name": "something_enum", "type": {"name": "inner_enum",
                                        "type": "enum",
                                        "symbols": ["hello", "goodbye"]}},
    {"name": "something_array", "type": {"type": "array", "items": "int"}},
    {"name": "something_map", "type": {"type": "map", "values": "int"}},
    {"name": "something_record", "type": {"name": "inner_record",
                                          "type": "record",
                                          "fields": [
                                            {"name": "inner", "type": "int"}
                                          ]}},
    {"name": "username", "type": "string"}
]}
JSON;

    $data = [
      [
        "username" => "john",
        "something_fixed" => "foo",
        "something_enum" => "hello",
        "something_array" => [1, 2, 3],
        "something_map" => ["a" => 1, "b" => 2],
        "something_record" => ["inner" => 2],
        "something_error" => ["code" => 403]
      ],
      [
        "username" => "ryan",
        "something_fixed" => "bar",
        "something_enum" => "goodbye",
        "something_array" => [1, 2, 3],
        "something_map" => ["a" => 2, "b" => 6],
        "something_record" => ["inner" => 1],
        "something_error" => ["code" => 401]
      ]
    ];
    $dw = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
    foreach ($data as $datum) {
      $dw->append($datum);
    }
    $dw->close();

//    foreach ([
//               'fixed',
//               'enum',
//               'record',
//               'error',
//               'array',
//               'map',
//               'union'
//             ] as $s) {
      $readersSchema = json_decode($writersSchema, true);
      $dr = AvroDataIO::openFile($dataFile, 'r', json_encode($readersSchema));
      foreach ($dr->data() as $idx => $obj) {
        foreach ($readersSchema['fields'] as $field) {
          $fieldName = $field['name'];
          $this->assertEquals($data[$idx][$fieldName], $obj[$fieldName]);
        }
      }
      $dr->close();
//    }

  }

}
