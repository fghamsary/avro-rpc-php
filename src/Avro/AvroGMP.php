<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:14
 */

namespace Avro;

/**
 * Methods for handling 64-bit operations using the GMP extension.
 *
 * This is a naive and hackish implementation that is intended
 * to work well enough to support Avro. It has not been tested
 * beyond what's needed to decode and encode long values.
 *
 * @package Avro
 */
class AvroGMP {

  /**
   * @var resource memoized GMP resource for zero
   */
  private static $gmp0;

  /**
   * @returns resource GMP resource for zero
   */
  private static function gmp0() {
    if (!isset(self::$gmp0)) {
      self::$gmp0 = gmp_init('0');
    }
    return self::$gmp0;
  }

  /**
   * @var resource memoized GMP resource for one (1)
   */
  private static $gmp1;

  /**
   * @returns resource GMP resource for one (1)
   */
  private static function gmp1() {
    if (!isset(self::$gmp1)) {
      self::$gmp1 = gmp_init('1');
    }
    return self::$gmp1;
  }

  /**
   * @var resource memoized GMP resource for two (2)
   */
  private static $gmp2;

  /**
   * @returns resource GMP resource for two (2)
   */
  private static function gmp2() {
    if (!isset(self::$gmp2)) {
      self::$gmp2 = gmp_init('2');
    }
    return self::$gmp2;
  }

  /**
   * @var resource memoized GMP resource for 0x7f
   */
  private static $gmp0x7f;

  /**
   * @returns resource GMP resource for 0x7f
   */
  private static function gmp0x7f() {
    if (!isset(self::$gmp0x7f)) {
      self::$gmp0x7f = gmp_init('0x7f');
    }
    return self::$gmp0x7f;
  }

  /**
   * @var resource memoized GMP resource for 64-bit ~0x7f
   */
  private static $gmpN0x7f;

  /**
   * @returns resource GMP resource for 64-bit ~0x7f
   */
  private static function gmpN0x7f() {
    if (!isset(self::$gmpN0x7f)) {
      self::$gmpN0x7f = gmp_init('0xffffffffffffff80');
    }
    return self::$gmpN0x7f;
  }

  /**
   * @var resource memoized GMP resource for 64-bits of 1
   */
  private static $gmp0xfs;

  /**
   * @returns resource GMP resource for 64-bits of 1
   */
  private static function gmp0xfs() {
    if (!isset(self::$gmp0xfs)) {
      self::$gmp0xfs = gmp_init('0xffffffffffffffff');
    }
    return self::$gmp0xfs;
  }

  /**
   * @param $g resource GMP
   * @return resource GMP 64-bit two's complement of input.
   */
  static function gmpTwosComplement($g) {
    return gmp_neg(gmp_sub(gmp_pow(self::gmp2(), 64), $g));
  }

  /**
   * @interal Only works up to shift 63 (doesn't wrap bits around).
   * @param resource|int|string $g
   * @param int $shift number of bits to shift left
   * @return resource $g shifted left
   */
  static function shiftLeft($g, $shift) {
    if (0 == $shift) {
      return $g;
    }

    if (0 > gmp_sign($g)) {
      $g = self::gmpTwosComplement($g);
    }

    $m = gmp_mul($g, gmp_pow(self::gmp2(), $shift));
    $m = gmp_and($m, self::gmp0xfs());
    if (gmp_testbit($m, 63)) {
      $m = gmp_neg(gmp_add(gmp_and(gmp_com($m), self::gmp0xfs()),
        self::gmp1()));
    }
    return $m;
  }

  /**
   * Arithmetic right shift
   * @param resource|int|string $g
   * @param int $shift number of bits to shift right
   * @return resource $g shifted right $shift bits
   */
  static function shiftRight($g, $shift) {
    if (0 == $shift) {
      return $g;
    }

    if (0 <= gmp_sign($g)) {
      $m = gmp_div($g, gmp_pow(self::gmp2(), $shift));
    } else { // negative
      $g = gmp_and($g, self::gmp0xfs());
      $m = gmp_div($g, gmp_pow(self::gmp2(), $shift));
      $m = gmp_and($m, self::gmp0xfs());
      for ($i = 63; $i >= (63 - $shift); $i--)
        gmp_setbit($m, $i);

      $m = gmp_neg(gmp_add(gmp_and(gmp_com($m), self::gmp0xfs()),
        self::gmp1()));
    }
    return $m;
  }

  /**
   * @param int|string $n integer (or string representation of integer) to encode
   * @return string $bytes of the long $n encoded per the Avro spec
   */
  static function encodeLong($n) {
    $g = gmp_init($n);
    $g = gmp_xor(self::shiftLeft($g, 1),
      self::shiftRight($g, 63));
    $bytes = '';
    while (0 != gmp_cmp(self::gmp0(), gmp_and($g, self::gmpN0x7f()))) {
      $bytes .= chr(gmp_intval(gmp_and($g, self::gmp0x7f())) | 0x80);
      $g = self::shiftRight($g, 7);
    }
    $bytes .= chr(gmp_intval($g));
    return $bytes;
  }

  /**
   * @param int[] $bytes array of ascii codes of bytes to decode
   * @return integer long representation of decoded long.
   */
  static function decodeLongFromArray($bytes) {
    $b = array_shift($bytes);
    $g = gmp_init($b & 0x7f);
    $shift = 7;
    while (0 != ($b & 0x80)) {
      $b = array_shift($bytes);
      $g = gmp_or($g, self::shiftLeft(($b & 0x7f), $shift));
      $shift += 7;
    }
    $val = gmp_xor(self::shiftRight($g, 1), gmp_neg(gmp_and($g, 1)));
    return (int)gmp_strval($val);
  }

}
