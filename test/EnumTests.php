<?php

use Avro\Exception\AvroException;
use Avro\Record\AvroEnumRecord;

/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 10/01/2019
 * Time: 15:01
 */

class EnumTests extends \PHPUnit\Framework\TestCase {

  public function testEnumDecoding() {
    $this->assertInstanceOf(TestEnum::class, TestEnum::getItem('FIRST_VALUE'));

    try {
      TestEnum::getItem('test');
      $this->fail("We should have seen an exception!");
    } catch (AvroException $exp) {
      $this->assertEquals('test is not valid for TestEnum!', $exp->getMessage());
    }
  }

}

class TestEnum extends AvroEnumRecord {

  public static function _getSimpleAvroClassName() {
    return 'TestEnum';
  }

  /**
   * @return string[] The list of available values for this enum
   */
  protected static function getEnumValues() {
    return [
      'FIRST_VALUE',
      'SECOND_VALUE',
      'THIRD_VALUE',
    ];
  }

  /**
   * FIRST_VALUE value for the enum.
   */
  public static function FIRST_VALUE() {
    return new TestEnum('FIRST_VALUE');
  }

  /**
   * SECOND_VALUE value for the enum.
   */
  public static function SECOND_VALUE() {
    return new TestEnum('SECOND_VALUE');
  }

  /**
   * THIRD_VALUE value for the enum.
   */
  public static function THIRD_VALUE() {
    return new TestEnum('THIRD_VALUE');
  }


}