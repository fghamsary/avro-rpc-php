<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:26
 */

namespace Avro;

use Avro\Exception\AvroException;

/**
 * Library-level class for PHP Avro port.
 *
 * Contains library details such as version number and platform checks.
 *
 * This port is an implementation of the
 * {@link http://avro.apache.org/docs/1.8.2/spec.html Avro 1.8.2 Specification}
 *
 * @package Avro
 */
class AvroSpec {
  /**
   * @var string version number of Avro specification to which this implementation complies
   */
  const SPEC_VERSION = '1.8.2';

  /**#@+
   * Constant to enumerate endianness.
   * @access private
   * @var int
   */
  const BIG_ENDIAN = 0x00;
  const LITTLE_ENDIAN = 0x01;
  /**#@-*/

  /**
   * Memoized result of self::set_endianness()
   * @var int self::BIG_ENDIAN or self::LITTLE_ENDIAN
   * @see self::set_endianness()
   */
  private static $endianness;

  /**#@+
   * Constant to enumerate biginteger handling mode.
   * GMP is used, if available, on 32-bit platforms.
   */
  const PHP_BIGINTEGER_MODE = 0x00;
  const GMP_BIGINTEGER_MODE = 0x01;
  /**#@-*/

  /**
   * @var int
   * Mode used to handle bigintegers. After AvroSpec::check_64_bit() has been called,
   * (usually via a call to AvroSpec::check_platform(), set to
   * self::GMP_BIGINTEGER_MODE on 32-bit platforms that have GMP available,
   * and to self::PHP_BIGINTEGER_MODE otherwise.
   */
  private static $biginteger_mode;

  /**
   * Wrapper method to call each required check.
   *
   * @throws AvroException
   */
  public static function checkPlatform() {
    self::check64Bit();
    self::checkLittleEndian();
  }

  /**
   * Determines if the host platform can encode and decode long integer data.
   *
   * @throws AvroException if the platform cannot handle long integers.
   */
  private static function check64Bit() {
    if (8 != PHP_INT_SIZE) {
      if (extension_loaded('gmp')) {
        self::$biginteger_mode = self::GMP_BIGINTEGER_MODE;
      } else {
        throw new AvroException('This platform cannot handle a 64-bit operations. '
          . 'Please install the GMP PHP extension.');
      }
    } else {
      self::$biginteger_mode = self::PHP_BIGINTEGER_MODE;
    }
  }

  /**
   * @return boolean true if the PHP GMP extension is used and false otherwise.
   * @internal Requires AvroSpec::check_64_bit() (exposed via AvroSpec::check_platform())
   *           to have been called to set AvroSpec::$biginteger_mode.
   */
  static function usesGmp() {
    return (self::GMP_BIGINTEGER_MODE === self::$biginteger_mode);
  }

  /**
   * Determines if the host platform is little endian,
   * required for processing double and float data.
   *
   * @throws AvroException if the platform is not little endian.
   */
  private static function checkLittleEndian() {
    if (!self::isLittleEndianPlatform()) {
      throw new AvroException('This is not a little-endian platform');
    }
  }

  /**
   * Determines the endianness of the host platform and memorizes the result to AvroSpec::$endianness.
   *
   * Based on a similar check performed in http://pear.php.net/package/Math_BinaryUtils
   *
   * @throws AvroException if the endianness cannot be determined.
   */
  private static function setEndianness() {
    $packed = pack('d', 1);
    switch ($packed) {
      case "\77\360\0\0\0\0\0\0":
        self::$endianness = self::BIG_ENDIAN;
        break;
      case "\0\0\0\0\0\0\360\77":
        self::$endianness = self::LITTLE_ENDIAN;
        break;
      default:
        throw new AvroException(
          sprintf('Error determining platform endianness: %s', AvroDebug::hexString($packed))
        );
    }
  }

  /**
   * @return boolean true if the host platform is big endian
   *                 and false otherwise.
   * @uses self::set_endianness()
   * @throws AvroException if the endianness cannot be determined.
   */
  private static function isBigEndianPlatform() {
    if (self::$endianness === null) {
      self::setEndianness();
    }
    return (self::BIG_ENDIAN === self::$endianness);
  }

  /**
   * @returns boolean true if the host platform is little endian,
   *                  and false otherwise.
   * @uses self::is_bin_endian_platform()
   * @throws AvroException  if the endianness cannot be determined.
   */
  private static function isLittleEndianPlatform() {
    return !self::isBigEndianPlatform();
  }

}
