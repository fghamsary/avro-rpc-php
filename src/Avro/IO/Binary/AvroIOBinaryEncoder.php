<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:02
 */

namespace Avro\IO;

use Avro\AvroGMP;
use Avro\AvroSpec;
use Avro\IO\Exception\AvroIOException;

/**
 * Class AvroIOBinaryEncoder
 *
 * Encodes and writes Avro data to an AvroIO object using Avro binary encoding.
 *
 * @package Avro\IO
 */
class AvroIOBinaryEncoder {

  /**
   * Performs encoding of the given float value to a binary string
   *
   * XXX: This is <b>not</b> endian-aware! The {@link AvroSpec::check_platform()}
   * called in {@link AvroIOBinaryEncoder::__construct()} should ensure the
   * library is only used on little-endian platforms, which ensure the little-endian
   * encoding required by the Avro spec.
   *
   * @param float $float
   * @return string bytes
   * @see AvroSpec::checkPlatform()
   */
  static function floatToIntBits($float) {
    return pack('f', (float)$float);
  }

  /**
   * Performs encoding of the given double value to a binary string
   *
   * XXX: This is <b>not</b> endian-aware! See comments in
   * {@link AvroIOBinaryEncoder::float_to_int_bits()} for details.
   *
   * @param double $double
   * @return string bytes
   */
  static function doubleToLongBits($double) {
    return pack('d', (double)$double);
  }

  /**
   * @param int|string $n
   * @return string long $n encoded as bytes
   * @internal This relies on 64-bit PHP.
   */
  static public function encodeLong($n) {
    $n = (int)$n;
    $n = ($n << 1) ^ ($n >> 63);
    $str = '';
    while (0 != ($n & ~0x7F)) {
      $str .= chr(($n & 0x7F) | 0x80);
      $n >>= 7;
    }
    $str .= chr($n);
    return $str;
  }

  /**
   * @var AvroIO
   */
  private $io;

  /**
   * @param AvroIO $io object to which data is to be written.
   *
   */
  function __construct($io) {
    AvroSpec::checkPlatform();
    $this->io = $io;
  }

  /**
   * @param boolean $datum
   * @throws AvroIOException
   */
  function writeBoolean($datum) {
    $byte = $datum ? chr(1) : chr(0);
    $this->write($byte);
  }

  /**
   * @param int $datum
   * @throws AvroIOException
   */
  function writeInt($datum) {
    $this->writeLong($datum);
  }

  /**
   * @param int $n
   * @throws AvroIOException
   */
  function writeLong($n) {
    if (AvroSpec::usesGmp()) {
      $this->write(AvroGMP::encode_long($n));
    } else {
      $this->write(self::encodeLong($n));
    }
  }

  /**
   * @param float $datum
   * @uses self::float_to_int_bits()
   * @throws AvroIOException
   */
  public function writeFloat($datum) {
    $this->write(self::floatToIntBits($datum));
  }

  /**
   * @param float $datum
   * @uses self::double_to_long_bits()
   * @throws AvroIOException
   */
  public function writeDouble($datum) {
    $this->write(self::doubleToLongBits($datum));
  }

  /**
   * @param string $str
   * @uses self::write_bytes()
   * @throws AvroIOException
   */
  function writeString($str) {
    $this->writeBytes($str);
  }

  /**
   * @param string $bytes
   * @throws AvroIOException
   */
  function writeBytes($bytes) {
    $this->writeLong(strlen($bytes));
    $this->write($bytes);
  }

  /**
   * @param string $datum
   * @throws AvroIOException
   */
  function write($datum) {
    $this->io->write($datum);
  }
}
