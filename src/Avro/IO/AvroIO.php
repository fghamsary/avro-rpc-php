<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:08
 */

namespace Avro\IO;

use Avro\IO\Exception\AvroIOException;

/**
 * Class AvroIO
 *
 * Barebone IO base class to provide common interface for file and string
 * access within the Avro classes.
 *
 * @package Avro\IO
 */
abstract class AvroIO {

  /**
   * @var string general read mode
   */
  const READ_MODE = 'r';
  /**
   * @var string general write mode.
   */
  const WRITE_MODE = 'w';

  /**
   * @var int set position equal to $offset bytes
   */
  const SEEK_CUR = SEEK_CUR;
  /**
   * @var int set position to current index + $offset bytes
   */
  const SEEK_SET = SEEK_SET;
  /**
   * @var int set position to end of file + $offset bytes
   */
  const SEEK_END = SEEK_END;

  /**
   * Read $len bytes from AvroIO instance
   * @var int $len
   * @return string bytes read
   * @throws AvroIOException
   */
  public abstract function read($len);

  /**
   * Append bytes to this buffer. (Nothing more is needed to support Avro.)
   * @param string $arg bytes to write
   * @return int count of bytes written.
   * @throws AvroIOException if $args is not a string value.
   */
  public abstract function write($arg);

  /**
   * Return byte offset within AvroIO instance
   * @return int
   */
  public abstract function tell();

  /**
   * Set the position indicator. The new position, measured in bytes
   * from the beginning of the file, is obtained by adding $offset to
   * the position specified by $whence.
   *
   * @param int $offset
   * @param int $whence one of AvroIO::SEEK_SET, AvroIO::SEEK_CUR,
   *                    or Avro::SEEK_END
   * @returns boolean true
   *
   * @throws AvroIOException
   */
  public abstract function seek($offset, $whence = self::SEEK_SET);

  /**
   * Flushes any buffered data to the AvroIO object.
   * @returns boolean true upon success.
   */
  public abstract function flush();

  /**
   * Returns whether or not the current position at the end of this AvroIO
   * instance.
   *
   * Note is_eof() is <b>not</b> like eof in C or feof in PHP:
   * it returns TRUE if the *next* read would be end of file,
   * rather than if the *most recent* read read end of file.
   * @returns boolean true if at the end of file, and false otherwise
   */
  public abstract function isEof();

  /**
   * Closes this AvroIO instance.
   */
  public abstract function close();
}
