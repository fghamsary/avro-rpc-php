<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:03
 */

namespace Avro\Record;

/**
 * Default implementation for the getter/setters of @see IAvroRecordBase
 * Trait TAvroRecordBase
 * @package Avro\Record
 */
trait TAvroRecordBase {

  private $iFieldsSet = 0;

  public function _internalGetValue(string $field) {
    $fieldGetter = 'get' . ucfirst($field);
    return $this->$fieldGetter();
  }

  public function _internalSetValue(string $field, $value) {
    $this->iFieldsSet++;
    $fieldSetter = 'set' . ucfirst($field);
    return $this->$fieldSetter($value);
  }

  /**
   * @return int the number of fields that have been set via _internalSetValue
   */
  public function count(): int {
    return $this->iFieldsSet;
  }

}
