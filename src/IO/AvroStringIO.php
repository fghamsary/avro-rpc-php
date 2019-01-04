<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:09
 */

namespace Avro\IO;

use Avro\IO\Exception\AvroIOException;

/**
 * Class AvroStringIO
 *
 * AvroIO wrapper for string access
 *
 * @package Avro\IO
 */
class AvroStringIO extends AvroIO {

  /**
   * @var string
   */
  private $stringBuffer;
  /**
   * @var int  current position in string
   */
  private $currentIndex;
  /**
   * @var boolean whether or not the string is closed.
   */
  private $isClosed;

  /**
   * @param string $str initial value of AvroStringIO buffer. Regardless
   *                    of the initial value, the pointer is set to the
   *                    beginning of the buffer.
   * @throws AvroIOException if a non-string value is passed as $str
   */
  public function __construct($str = '') {
    $this->isClosed = false;
    $this->stringBuffer = '';
    $this->currentIndex = 0;

    if (is_string($str)) {
      $this->stringBuffer .= $str;
    }
    else {
      throw new AvroIOException(sprintf('constructor argument must be a string: %s', gettype($str)));
    }
  }

  /**
   * Append bytes to this buffer.
   * (Nothing more is needed to support Avro.)
   * @param string $arg bytes to write
   * @return int count of bytes written.
   * @throws AvroIOException if $args is not a string value.
   */
  public function write($arg) {
    $this->checkClosed();
    if (is_string($arg)) {
      return $this->appendString($arg);
    }
    throw new AvroIOException(
      sprintf('write argument must be a string: (%s) %s', gettype($arg), var_export($arg, true))
    );
  }

  /**
   * @param $len
   * @return string bytes read from buffer
   * @throws AvroIOException
   * @todo test for fencepost errors wrt updating current_index
   */
  public function read($len) {
    $this->checkClosed();
    $read='';
    for($i=$this->currentIndex; $i<($this->currentIndex+$len); $i++)
      $read .= $this->stringBuffer[$i];
    if (strlen($read) < $len)
      $this->currentIndex = $this->length();
    else
      $this->currentIndex += $len;
    return $read;
  }

  /**
   * @param int $offset
   * @param int $whence
   *
   * @return boolean true if successful
   * @throws AvroIOException if the seek failed.
   */
  public function seek($offset, $whence = self::SEEK_SET) {
    if (!is_int($offset)) {
      throw new AvroIOException('Seek offset must be an integer.');
    }
    // Prevent seeking before BOF
    switch ($whence) {
      case self::SEEK_SET:
        if (0 > $offset)
          throw new AvroIOException('Cannot seek before beginning of file.');
        $this->currentIndex = $offset;
        break;
      case self::SEEK_CUR:
        if (0 > $this->currentIndex + $whence)
          throw new AvroIOException('Cannot seek before beginning of file.');
        $this->currentIndex += $offset;
        break;
      case self::SEEK_END:
        if (0 > $this->length() + $offset)
          throw new AvroIOException('Cannot seek before beginning of file.');
        $this->currentIndex = $this->length() + $offset;
        break;
      default:
        throw new AvroIOException(sprintf('Invalid seek whence %d', $whence));
    }

    return true;
  }

  /**
   * @returns int
   * @see AvroIO::tell()
   */
  public function tell() {
    return $this->currentIndex;
  }

  /**
   * @returns boolean
   * @see AvroIO::isEof()
   */
  public function isEof() {
    return ($this->currentIndex >= $this->length());
  }

  /**
   * No-op provided for compatibility with AvroIO interface.
   * @returns boolean true
   */
  public function flush() {
    return true;
  }

  /**
   * Marks this buffer as closed.
   * @return boolean true
   *
   * @throws AvroIOException
   */
  public function close() {
    $this->checkClosed();
    $this->isClosed = true;
    return true;
  }

  /**
   * @throws AvroIOException if the buffer is closed.
   */
  private function checkClosed() {
    if ($this->isClosed()) {
      throw new AvroIOException('Buffer is closed');
    }
  }

  /**
   * Appends bytes to this buffer.
   * @param string $str
   * @return integer count of bytes written.
   * @throws AvroIOException
   */
  private function appendString($str) {
    $this->checkClosed();
    $this->stringBuffer .= $str;
    $len = strlen($str);
    $this->currentIndex += $len;
    return $len;
  }

  /**
   * Truncates the truncate buffer to 0 bytes and returns the pointer
   * to the beginning of the buffer.
   * @return boolean true
   * @throws AvroIOException
   */
  public function truncate() {
    $this->checkClosed();
    $this->stringBuffer = '';
    $this->currentIndex = 0;
    return true;
  }

  /**
   * @return int count of bytes in the buffer
   * @internal Could probably memoize length for performance, but
   *           no need do this yet.
   */
  public function length() {
    return strlen($this->stringBuffer);
  }

  /**
   * @return string
   */
  public function __toString() {
    return $this->stringBuffer;
  }

  /**
   * @return string
   * @uses self::__toString()
   */
  public function string() {
    return $this->__toString();
  }

  /**
   * @return boolean true if this buffer is closed and false
   *                       otherwise.
   */
  public function isClosed() {
    return $this->isClosed;
  }

}
