<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:15
 */

namespace Avro;

/**
 * Class AvroUtil
 *
 * Class for static utility methods used in Avro.
 *
 * @package Avro
 */
class AvroUtil {

  /**
   * Determines whether the given array is an associative array
   * (what is termed a map, hash, or dictionary in other languages)
   * or a list (an array with monotonically increasing integer indicies
   * starting with zero).
   *
   * @param array $ary array to test
   * @return true if the array is a list and false otherwise.
   *
   */
  static function isList($ary) {
    if (is_array($ary)) {
      $i = 0;
      foreach ($ary as $k => $v) {
        if ($i !== $k) {
          return false;
        }
        $i++;
      }
      return true;
    }
    return false;
  }

  /**
   * @param array $ary the array which we should take a look at
   * @param string $key the which should be checked in the array
   * @return mixed the value of $ary[$key] if it is set, and null otherwise.
   */
  static function arrayValue($ary, $key) {
    return isset($ary[$key]) ? $ary[$key] : null;
  }

}