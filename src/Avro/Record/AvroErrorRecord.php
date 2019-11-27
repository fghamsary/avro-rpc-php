<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:05
 */

namespace Avro\Record;

use Avro\Exception\AvroRemoteException;

/**
 * Class AvroErrorRecord base class for all Exceptions defined as error in the avro schema
 * @package Avro\Record
 */
abstract class AvroErrorRecord extends AvroRemoteException implements IAvroRecordBase {
  use TAvroRecordBase;

  public abstract static function _getSimpleAvroClassName(): string;

  /**
   * @return static Creates new instance of the current error type
   */
  public static function newInstance() {
    return new static();
  }
}
