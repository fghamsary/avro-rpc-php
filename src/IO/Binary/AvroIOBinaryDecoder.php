<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:04
 */

namespace Avro\IO;

use Avro\AvroGMP;
use Avro\AvroSpec;
use Avro\Exception\AvroException;
use Avro\IO\Exception\AvroIOException;

/**
 * Class AvroIOBinaryDecoder
 *
 * Decodes and reads Avro data from an AvroIO object encoded using Avro binary encoding.
 *
 * @package Avro\IO
 */
class AvroIOBinaryDecoder {

  /**
   * @param int[] array of byte ascii values
   * @return int long decoded value
   * @internal Requires 64-bit platform
   */
  public static function decode_long_from_array($bytes) {
    $b = array_shift($bytes);
    $n = $b & 0x7f;
    $shift = 7;
    while (0 != ($b & 0x80)) {
      $b = array_shift($bytes);
      $n |= (($b & 0x7f) << $shift);
      $shift += 7;
    }
    return (($n >> 1) ^ -($n & 1));
  }

  /**
   * Performs decoding of the binary string to a float value.
   *
   * XXX: This is <b>not</b> endian-aware! See comments in
   * {@link AvroIOBinaryEncoder::float_to_int_bits()} for details.
   *
   * @param string $bits
   * @return float
   */
  static public function int_bits_to_float($bits) {
    $float = unpack('f', $bits);
    return (float)$float[1];
  }

  /**
   * Performs decoding of the binary string to a double value.
   *
   * XXX: This is <b>not</b> endian-aware! See comments in
   * {@link AvroIOBinaryEncoder::float_to_int_bits()} for details.
   *
   * @param string $bits
   * @return float
   */
  static public function long_bits_to_double($bits) {
    $double = unpack('d', $bits);
    return (double)$double[1];
  }

  /**
   * @var AvroIO
   */
  private $io;

  /**
   * @param AvroIO $io object from which to read.
   */
  public function __construct(AvroIO $io) {
    AvroSpec::checkPlatform();
    $this->io = $io;
  }

  /**
   * @return string the next byte from $this->io.
   * @throws AvroException if the next byte cannot be read.
   */
  private function next_byte() {
    return $this->read(1);
  }

  /**
   * @return null
   */
  public function read_null() {
    return null;
  }

  /**
   * @return boolean
   * @throws AvroException
   */
  public function read_boolean() {
    return (boolean)(1 == ord($this->next_byte()));
  }

  /**
   * @return int
   * @throws AvroException
   */
  public function read_int() {
    return (int)$this->read_long();
  }

  /**
   * @return int long
   * @throws AvroException
   */
  public function read_long() {
    $byte = ord($this->next_byte());
    $bytes = array($byte);
    while (0 != ($byte & 0x80)) {
      $byte = ord($this->next_byte());
      $bytes [] = $byte;
    }

    if (AvroSpec::usesGmp()) {
      return AvroGMP::decode_long_from_array($bytes);
    }

    return self::decode_long_from_array($bytes);
  }

  /**
   * @return float
   * @throws AvroIOException
   */
  public function read_float() {
    return self::int_bits_to_float($this->read(4));
  }

  /**
   * @return double
   * @throws AvroIOException
   */
  public function read_double() {
    return self::long_bits_to_double($this->read(8));
  }

  /**
   * A string is encoded as a long followed by that many bytes
   * of UTF-8 encoded character data.
   * @return string
   * @throws AvroException
   */
  public function read_string() {
    return $this->read_bytes();
  }

  /**
   * @return string
   * @throws AvroException
   */
  public function read_bytes() {
    return $this->read($this->read_long());
  }

  /**
   * @param int $len count of bytes to read
   * @return string
   * @throws AvroIOException
   */
  public function read($len) {
    return $this->io->read($len);
  }

  public function skip_null() {
    null;
  }

  /**
   * @throws AvroIOException
   */
  public function skip_boolean() {
    $this->skip(1);
  }

  /**
   * @throws AvroException
   */
  public function skip_int() {
    $this->skip_long();
  }

  /**
   * @throws AvroException
   */
  public function skip_long() {
    $b = $this->next_byte();
    while (0 != ((int)$b & 0x80))
      $b = $this->next_byte();
  }

  /**
   * @throws AvroIOException
   */
  public function skip_float() {
    $this->skip(4);
  }

  /**
   * @throws AvroIOException
   */
  public function skip_double() {
    $this->skip(8);
  }

  /**
   * @throws AvroException
   */
  public function skip_bytes() {
    $this->skip($this->read_long());
  }

  /**
   * @throws AvroException
   */
  public function skip_string() {
    $this->skip_bytes();
  }

  /**
   * @param int $len count of bytes to skip
   * @uses AvroIO::seek()
   * @throws AvroIOException
   */
  public function skip($len) {
    $this->seek($len, AvroIO::SEEK_CUR);
  }

  /**
   * @return int position of pointer in AvroIO instance
   * @uses AvroIO::tell()
   */
  public function tell() {
    return $this->io->tell();
  }

  /**
   * @param int $offset
   * @param int $whence
   * @return boolean true upon success
   * @uses AvroIO::seek()
   * @throws AvroIOException
   */
  private function seek($offset, $whence) {
    return $this->io->seek($offset, $whence);
  }
}
