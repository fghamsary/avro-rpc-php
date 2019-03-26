<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:33
 */

namespace Avro;

use Avro\Exception\AvroException;

/**
 * Class AvroDebug
 *
 * Avro library code debugging functions
 *
 * @package Avro
 */
class AvroDebug {

  /**
   * @var int high debug level
   */
  const DEBUG5 = 5;
  /**
   * @var int low debug level
   */
  const DEBUG1 = 1;
  /**
   * @var int current debug level
   */
  const DEBUG_LEVEL = self::DEBUG1;

  /**
   * @var int $debugLevel
   * @returns boolean true if the given $debug_level is equivalent
   *                  or more verbose than than the current debug level
   *                  and false otherwise.
   * @return bool
   */
  static function isDebug($debugLevel = self::DEBUG1) {
    return (self::DEBUG_LEVEL >= $debugLevel);
  }

  /**
   * @param string $format format string for the given arguments. Passed as is
   *                     to <code>vprintf</code>.
   * @param array $args array of arguments to pass to vsprinf.
   * @param int $debugLevel debug level at which to print this statement
   * @return boolean true
   */
  static function debug($format, $args, $debugLevel = self::DEBUG1) {
    if (self::isDebug($debugLevel)) {
      vprintf($format . "\n", $args);
    }
    return true;
  }

  /**
   * @param string $str
   * @return string[] array of hex representation of each byte of $str
   */
  static function hexArray($str) {
    return self::bytesArray($str);
  }

  /**
   * @param string $str
   * @param string $joiner string used to join
   * @return string hex-represented bytes of each byte of $str
   * joined by $joiner
   */
  static function hexString($str, $joiner = ' ') {
    return join($joiner, self::hexArray($str));
  }

  /**
   * @param string $str
   * @param string $format format to represent bytes
   * @return string[] array of each byte of $str formatted using $format
   */
  static function bytesArray($str, $format = 'x%02x') {
    $x = [];
    foreach (str_split($str) as $b) {
      $x [] = sprintf($format, ord($b));
    }
    return $x;
  }

  /**
   * @param string $str
   * @return string[] array of bytes of $str represented in decimal format ('%3d')
   */
  static function decArray($str) {
    return self::bytesArray($str, '%3d');
  }

  /**
   * @param string $str
   * @param string $joiner string to join bytes of $str
   * @return string of bytes of $str represented in decimal format
   * @uses decArray()
   */
  static function decString($str, $joiner = ' ') {
    return join($joiner, self::decArray($str));
  }

  /**
   * @param string $str
   * @param string $format one of 'ctrl', 'hex', or 'dec' for control,
   * hexadecimal, or decimal format for bytes.
   * - ctrl: ASCII control characters represented as text.
   * For example, the null byte is represented as 'NUL'.
   * Visible ASCII characters represent themselves, and
   * others are represented as a decimal ('%03d')
   * - hex: bytes represented in hexadecimal ('%02X')
   * - dec: bytes represented in decimal ('%03d')
   * @return string[] array of bytes represented in the given format.
   * @throws AvroException
   */
  static function asciiArray($str, $format = 'ctrl') {
    if (!in_array($format, ['ctrl', 'hex', 'dec'])) {
      throw new AvroException('Unrecognized format specifier');
    }

    $ctrl_chars = [
      'NUL',
      'SOH',
      'STX',
      'ETX',
      'EOT',
      'ENQ',
      'ACK',
      'BEL',
      'BS',
      'HT',
      'LF',
      'VT',
      'FF',
      'CR',
      'SO',
      'SI',
      'DLE',
      'DC1',
      'DC2',
      'DC3',
      'DC4',
      'NAK',
      'SYN',
      'ETB',
      'CAN',
      'EM',
      'SUB',
      'ESC',
      'FS',
      'GS',
      'RS',
      'US'
    ];
    $x = [];
    foreach (str_split($str) as $b) {
      $db = ord($b);
      if ($db < 32) {
        switch ($format) {
          case 'ctrl':
            $x [] = str_pad($ctrl_chars[$db], 3, ' ', STR_PAD_LEFT);
            break;
          case 'hex':
            $x [] = sprintf("x%02X", $db);
            break;
          case 'dec':
            $x [] = str_pad($db, 3, '0', STR_PAD_LEFT);
            break;
        }
      } else {
        if ($db < 127) {
          $x [] = "  $b";
        } else {
          if ($db == 127) {
            switch ($format) {
              case 'ctrl':
                $x [] = 'DEL';
                break;
              case 'hex':
                $x [] = sprintf("x%02X", $db);
                break;
              case 'dec':
                $x [] = str_pad($db, 3, '0', STR_PAD_LEFT);
                break;
            }
          } else {
            if ('hex' == $format) {
              $x [] = sprintf("x%02X", $db);
            } else {
              $x [] = str_pad($db, 3, '0', STR_PAD_LEFT);
            }
          }
        }
      }
    }
    return $x;
  }

  /**
   * @param string $str
   * @param string $format one of 'ctrl', 'hex', or 'dec'.
   *                       See {@link self::ascii_array()} for more description
   * @param string $joiner
   * @return string of bytes joined by $joiner
   * @uses asciiArray()
   * @throws AvroException
   */
  static function asciiString($str, $format = 'ctrl', $joiner = ' '): string {
    return join($joiner, self::asciiArray($str, $format));
  }
}
