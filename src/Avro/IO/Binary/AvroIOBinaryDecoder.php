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
  public static function decodeLongFromArray($bytes) {
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
   * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
   *
   * @param string $bits
   * @return float
   */
  static public function intBitsToFloat($bits) {
    $float = unpack('f', $bits);
    return (float) $float[1];
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
  static public function longBitsToDouble($bits) {
    $double = unpack('d', $bits);
    return (double) $double[1];
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
   * @throws AvroIOException if the next byte cannot be read.
   */
  private function nextByte() {
    return $this->read(1);
  }

  /**
   * @return null
   */
  public function readNull() {
    return null;
  }

  /**
   * @return boolean
   * @throws AvroIOException
   */
  public function readBoolean() {
    return (boolean) (1 == ord($this->nextByte()));
  }

  /**
   * @return int
   * @throws AvroIOException
   */
  public function readInt() {
    return (int) $this->readLong();
  }

  /**
   * @return int long
   * @throws AvroIOException
   */
  public function readLong() {
    $byte = ord($this->nextByte());
    $bytes = array($byte);
    while (0 != ($byte & 0x80)) {
      $byte = ord($this->nextByte());
      $bytes[] = $byte;
    }

    if (AvroSpec::usesGmp()) {
      return AvroGMP::decodeLongFromArray($bytes);
    }

    return self::decodeLongFromArray($bytes);
  }

  /**
   * @return float
   * @throws AvroIOException
   */
  public function readFloat() {
    return self::intBitsToFloat($this->read(4));
  }

  /**
   * @return double
   * @throws AvroIOException
   */
  public function readDouble() {
    return self::longBitsToDouble($this->read(8));
  }

  /**
   * A string is encoded as a long followed by that many bytes
   * of UTF-8 encoded character data.
   * @return string
   * @throws AvroIOException
   */
  public function readString() {
    return $this->readBytes();
  }

  /**
   * @return string
   * @throws AvroIOException
   */
  public function readBytes() {
    return $this->read($this->readLong());
  }

  /**
   * @param int $len count of bytes to read
   * @return string
   * @throws AvroIOException
   */
  public function read($len) {
    return $this->io->read($len);
  }

  /**
   * @throws AvroIOException
   */
  public function skipBoolean() {
    $this->skip(1);
  }

  /**
   * @throws AvroIOException
   */
  public function skipInt() {
    $this->skipLong();
  }

  /**
   * @throws AvroIOException
   */
  public function skipLong() {
    $b = $this->nextByte();
    while (0 != ((int) $b & 0x80)) {
      $b = $this->nextByte();
    }
  }

  /**
   * @throws AvroIOException
   */
  public function skipFloat() {
    $this->skip(4);
  }

  /**
   * @throws AvroIOException
   */
  public function skipDouble() {
    $this->skip(8);
  }

  /**
   * @throws AvroIOException
   */
  public function skipBytes() {
    $this->skip($this->readLong());
  }

  /**
   * @throws AvroIOException
   */
  public function skipString() {
    $this->skipBytes();
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
