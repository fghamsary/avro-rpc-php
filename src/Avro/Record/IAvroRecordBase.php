<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 10:59
 */

namespace Avro\Record;

/**
 * Base interface for AvroNamedSchema bases like record and error
 * Interface IAvroRecordBase
 * @package Avro\Record
 */
interface IAvroRecordBase extends \Countable {
  /**
   * @return string Get the name of the current record type
   */
  public static function _getSimpleAvroClassName();

  /**
   * @return $this Instances new record based on the type selected
   */
  public static function newInstance();

  /**
   * @param string $field The field which we want the value for
   * @return mixed
   */
  public function _internalGetValue($field);

  /**
   * @param string $field The field which should be set
   * @param mixed $value The value which should be set
   * @return $this
   */
  public function _internalSetValue($field, $value);
}
