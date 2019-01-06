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

use Avro\IO\AvroFile;
use Avro\Schema\AvroSchema;
use Avro\IO\Data\AvroDataIO;

require_once('test_helper.php');

class InterOpTest extends PHPUnit\Framework\TestCase {
  var $projectionJson;
  var $projection;

  public function setUp() {
    $interopSchemaFileName = AVRO_INTEROP_SCHEMA;
    $this->projectionJson = file_get_contents($interopSchemaFileName);
    $this->projection = AvroSchema::parse($this->projectionJson);
  }

  public function fileNameProvider() {
    $dataDir = AVRO_DATA_DIR;
    $dataFiles = [];
    if (!($dh = opendir($dataDir))) {
      die("Could not open data dir '$dataDir'\n");
    }

    /* TODO This currently only tries to read files of the form 'language.avro',
     * but not 'language_deflate.avro' as the PHP implementation is not yet
     * able to read deflate data files. When deflate support is added, change
     * this to match *.avro. */
    while ($file = readdir($dh)) {
      if (0 < preg_match('/^[a-z]+\.avro$/', $file)) {
        $dataFiles[] = join(DIRECTORY_SEPARATOR, [$dataDir, $file]);
      }
    }
    closedir($dh);

    $ary = [];
    foreach ($dataFiles as $df) {
      $ary[] = [$df];
    }
    return $ary;
  }

  /**
   * @dataProvider fileNameProvider
   */
  public function testRead($fileName) {
    $dr = AvroDataIO::openFile($fileName, AvroFile::READ_MODE, $this->projectionJson);

    $data = $dr->data();

    $this->assertNotEquals(0, count($data), sprintf("no data read from %s", $fileName));

    foreach ($data as $idx => $datum) {
      $this->assertNotNull($datum, sprintf("null datum from %s", $fileName));
    }
  }

}
