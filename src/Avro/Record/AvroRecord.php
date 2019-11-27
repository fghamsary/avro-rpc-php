<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:04
 */

namespace Avro\Record;

/**
 * Base class for AvroRecord such as error/record
 * Class AvroRecord
 * @package Avro\Record
 */
abstract class AvroRecord implements IAvroRecordBase {
  use TAvroRecordBase;

  public abstract static function _getSimpleAvroClassName(): string;

  /**
   * @return static Creates new instance of the current record type
   */
  public static function newInstance() {
    return new static();
  }

}
