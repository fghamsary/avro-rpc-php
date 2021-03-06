<?php /** @noinspection PhpUnusedParameterInspection */

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

use Avro\AvroGMP;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\AvroSpec;

require_once('test_helper.php');

class LongEncodingTest extends PHPUnit\Framework\TestCase {

  /**
   * @throws \Avro\Exception\AvroException
   */
  function setUp() {
    AvroSpec::checkPlatform();
  }

  static function is64Bit() {
    return PHP_INT_SIZE === 8;
  }

  function skip64BitTestOn32Bit() {
    if (!self::is64Bit()) {
      $this->markTestSkipped('Requires 64-bit platform');
    }
  }

  function skipIfNoGmp() {
    if (!extension_loaded('gmp')) {
      $this->markTestSkipped('Requires GMP PHP Extension.');
    }
  }

  function assertBitShift($expected, $actual, $shift_type, $expected_binary, $actual_binary) {
    $this->assertEquals(
      $expected, $actual,
      sprintf("%s\nexpected: %d\n  actual: %d\nexpected b: %s\n  actual b: %s",
        $shift_type, $expected, $actual,
        $expected_binary, $actual_binary));
  }

  /**
   * @dataProvider bitShiftProvider
   * @param $val
   * @param $shift
   * @param $expectedLVal
   * @param $expectedRVal
   * @param $lBin
   * @param $rBin
   */
  function testBitShift($val, $shift, $expectedLVal, $expectedRVal, $lBin, $rBin) {

    $this->skip64BitTestOn32Bit();

    $lVal = (int)((int)$val << $shift);
    $this->assertBitShift($expectedLVal, strval($lVal), 'lshift', $lBin, decbin($lVal));
    $rVal = ((int)$val >> $shift);
    $this->assertBitShift($expectedRVal, strval($rVal), 'rshift', $rBin, decbin($rVal));
  }

  /**
   * @dataProvider bitShiftProvider
   * @param $val
   * @param $shift
   * @param $expectedLVal
   * @param $expectedRVal
   * @param $lBin
   * @param $rBin
   */
  function testLeftShiftGmp($val, $shift,
                            $expectedLVal, $expectedRVal,
                            $lBin, $rBin) {
    $this->skipIfNoGmp();
    $lVal = gmp_strval(AvroGMP::shiftLeft($val, $shift));
    $this->assertBitShift($expectedLVal, $lVal, 'gmp left shift', $lBin, decbin((int)$lVal));
  }

  /**
   * @dataProvider bitShiftProvider
   * @param $val
   * @param $shift
   * @param $expectedLVal
   * @param $expectedRVal
   * @param $lBin
   * @param $rBin
   */
  function testRightShiftGmp($val, $shift, $expectedLVal, $expectedRVal, $lBin, $rBin) {
    $this->skipIfNoGmp();
    $rVal = gmp_strval(AvroGMP::shiftRight($val, $shift));
    $this->assertBitShift($expectedRVal, $rVal, 'gmp right shift', $rBin, decbin((int)$rVal));
  }

  /**
   * @dataProvider longProvider
   * @param $val
   * @param $expectedBytes
   */
  function testEncodeLong($val, $expectedBytes) {
    $this->skip64BitTestOn32Bit();
    $bytes = AvroIOBinaryEncoder::encodeLong($val);
    $this->assertEquals($expectedBytes, $bytes);
  }

  /**
   * @dataProvider longProvider
   * @param $val
   * @param $expected_bytes
   */
  function testGmpEncodeLong($val, $expected_bytes) {
    $this->skipIfNoGmp();
    $bytes = AvroGMP::encodeLong($val);
    $this->assertEquals($expected_bytes, $bytes);
  }

  /**
   * @dataProvider longProvider
   * @param $expectedVal
   * @param $bytes
   */
  function testDecodeLongFromArray($expectedVal, $bytes) {
    $this->skip64BitTestOn32Bit();
    $ary = array_map('ord', str_split($bytes));
    $val = AvroIOBinaryDecoder::decodeLongFromArray($ary);
    $this->assertEquals($expectedVal, $val);
  }

  /**
   * @dataProvider longProvider
   * @param $expectedVal
   * @param $bytes
   */
  function testGmpDecodeLongFromArray($expectedVal, $bytes) {
    $this->skipIfNoGmp();
    $ary = array_map('ord', str_split($bytes));
    $val = AvroGMP::decodeLongFromArray($ary);
    $this->assertEquals($expectedVal, $val);
  }

  function longProvider() {
    return [
      ['0', "\x0"],
      ['1', "\x2"],
      ['7', "\xe"],
      ['10000', "\xa0\x9c\x1"],
      ['2147483647', "\xfe\xff\xff\xff\xf"],
      ['98765432109', "\xda\x94\x87\xee\xdf\x5"],
      ['-1', "\x1"],
      ['-7', "\xd"],
      ['-10000', "\x9f\x9c\x1"],
      ['-2147483648', "\xff\xff\xff\xff\xf"],
      ['-98765432109', "\xd9\x94\x87\xee\xdf\x5"]
    ];

  }

  function bitShiftProvider() {
    // val shift lVal rVal
    return [
      [
        '0',
        0,
        '0',
        '0',
        '0',
        '0'
      ],
      [
        '0',
        1,
        '0',
        '0',
        '0',
        '0'
      ],
      [
        '0',
        7,
        '0',
        '0',
        '0',
        '0'
      ],
      [
        '0',
        63,
        '0',
        '0',
        '0',
        '0'
      ],
      [
        '1',
        0,
        '1',
        '1',
        '1',
        '1'
      ],
      [
        '1',
        1,
        '2',
        '0',
        '10',
        '0'
      ],
      [
        '1',
        7,
        '128',
        '0',
        '10000000',
        '0'
      ],
      [
        '1',
        63,
        '-9223372036854775808',
        '0',
        '1000000000000000000000000000000000000000000000000000000000000000',
        '0'
      ],
      [
        '100',
        0,
        '100',
        '100',
        '1100100',
        '1100100'
      ],
      [
        '100',
        1,
        '200',
        '50',
        '11001000',
        '110010'
      ],
      [
        '100',
        7,
        '12800',
        '0',
        '11001000000000',
        '0'
      ],
      [
        '100',
        63,
        '0',
        '0',
        '0',
        '0'
      ],
      [
        '1000000',
        0,
        '1000000',
        '1000000',
        '11110100001001000000',
        '11110100001001000000'
      ],
      [
        '1000000',
        1,
        '2000000',
        '500000',
        '111101000010010000000',
        '1111010000100100000'
      ],
      [
        '1000000',
        7,
        '128000000',
        '7812',
        '111101000010010000000000000',
        '1111010000100'
      ],
      [
        '1000000',
        63,
        '0',
        '0',
        '0',
        '0'
      ],
      [
        '2147483647',
        0,
        '2147483647',
        '2147483647',
        '1111111111111111111111111111111',
        '1111111111111111111111111111111'
      ],
      [
        '2147483647',
        1,
        '4294967294',
        '1073741823',
        '11111111111111111111111111111110',
        '111111111111111111111111111111'
      ],
      [
        '2147483647',
        7,
        '274877906816',
        '16777215',
        '11111111111111111111111111111110000000',
        '111111111111111111111111'
      ],
      [
        '2147483647',
        63,
        '-9223372036854775808',
        '0',
        '1000000000000000000000000000000000000000000000000000000000000000',
        '0'
      ],
      [
        '10000000000',
        0,
        '10000000000',
        '10000000000',
        '1001010100000010111110010000000000',
        '1001010100000010111110010000000000'
      ],
      [
        '10000000000',
        1,
        '20000000000',
        '5000000000',
        '10010101000000101111100100000000000',
        '100101010000001011111001000000000'
      ],
      [
        '10000000000',
        7,
        '1280000000000',
        '78125000',
        '10010101000000101111100100000000000000000',
        '100101010000001011111001000'
      ],
      [
        '10000000000',
        63,
        '0',
        '0',
        '0',
        '0'
      ],
      [
        '9223372036854775807',
        0,
        '9223372036854775807',
        '9223372036854775807',
        '111111111111111111111111111111111111111111111111111111111111111',
        '111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '9223372036854775807',
        1,
        '-2',
        '4611686018427387903',
        '1111111111111111111111111111111111111111111111111111111111111110',
        '11111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '9223372036854775807',
        7,
        '-128',
        '72057594037927935',
        '1111111111111111111111111111111111111111111111111111111110000000',
        '11111111111111111111111111111111111111111111111111111111'
      ],
      [
        '9223372036854775807',
        63,
        '-9223372036854775808',
        '0',
        '1000000000000000000000000000000000000000000000000000000000000000',
        '0'
      ],
      [
        '-1',
        0,
        '-1',
        '-1',
        '1111111111111111111111111111111111111111111111111111111111111111',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-1',
        1,
        '-2',
        '-1',
        '1111111111111111111111111111111111111111111111111111111111111110',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-1',
        7,
        '-128',
        '-1',
        '1111111111111111111111111111111111111111111111111111111110000000',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-1',
        63,
        '-9223372036854775808',
        '-1',
        '1000000000000000000000000000000000000000000000000000000000000000',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-100',
        0,
        '-100',
        '-100',
        '1111111111111111111111111111111111111111111111111111111110011100',
        '1111111111111111111111111111111111111111111111111111111110011100'
      ],
      [
        '-100',
        1,
        '-200',
        '-50',
        '1111111111111111111111111111111111111111111111111111111100111000',
        '1111111111111111111111111111111111111111111111111111111111001110'
      ],
      [
        '-100',
        7,
        '-12800',
        '-1',
        '1111111111111111111111111111111111111111111111111100111000000000',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-100',
        63,
        '0',
        '-1',
        '0',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-1000000',
        0,
        '-1000000',
        '-1000000',
        '1111111111111111111111111111111111111111111100001011110111000000',
        '1111111111111111111111111111111111111111111100001011110111000000'
      ],
      [
        '-1000000',
        1,
        '-2000000',
        '-500000',
        '1111111111111111111111111111111111111111111000010111101110000000',
        '1111111111111111111111111111111111111111111110000101111011100000'
      ],
      [
        '-1000000',
        7,
        '-128000000',
        '-7813',
        '1111111111111111111111111111111111111000010111101110000000000000',
        '1111111111111111111111111111111111111111111111111110000101111011'
      ],
      [
        '-1000000',
        63,
        '0',
        '-1',
        '0',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-2147483648',
        0,
        '-2147483648',
        '-2147483648',
        '1111111111111111111111111111111110000000000000000000000000000000',
        '1111111111111111111111111111111110000000000000000000000000000000'
      ],
      [
        '-2147483648',
        1,
        '-4294967296',
        '-1073741824',
        '1111111111111111111111111111111100000000000000000000000000000000',
        '1111111111111111111111111111111111000000000000000000000000000000'
      ],
      [
        '-2147483648',
        7,
        '-274877906944',
        '-16777216',
        '1111111111111111111111111100000000000000000000000000000000000000',
        '1111111111111111111111111111111111111111000000000000000000000000'
      ],
      [
        '-2147483648',
        63,
        '0',
        '-1',
        '0',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-10000000000',
        0,
        '-10000000000',
        '-10000000000',
        '1111111111111111111111111111110110101011111101000001110000000000',
        '1111111111111111111111111111110110101011111101000001110000000000'
      ],
      [
        '-10000000000',
        1,
        '-20000000000',
        '-5000000000',
        '1111111111111111111111111111101101010111111010000011100000000000',
        '1111111111111111111111111111111011010101111110100000111000000000'
      ],
      [
        '-10000000000',
        7,
        '-1280000000000',
        '-78125000',
        '1111111111111111111111101101010111111010000011100000000000000000',
        '1111111111111111111111111111111111111011010101111110100000111000'
      ],
      [
        '-10000000000',
        63,
        '0',
        '-1',
        '0',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
      [
        '-9223372036854775808',
        0,
        '-9223372036854775808',
        '-9223372036854775808',
        '1000000000000000000000000000000000000000000000000000000000000000',
        '1000000000000000000000000000000000000000000000000000000000000000'
      ],
      [
        '-9223372036854775808',
        1,
        '0',
        '-4611686018427387904',
        '0',
        '1100000000000000000000000000000000000000000000000000000000000000'
      ],
      [
        '-9223372036854775808',
        7,
        '0',
        '-72057594037927936',
        '0',
        '1111111100000000000000000000000000000000000000000000000000000000'
      ],
      [
        '-9223372036854775808',
        63,
        '0',
        '-1',
        '0',
        '1111111111111111111111111111111111111111111111111111111111111111'
      ],
    ];
  }

}
