<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 02/01/2019
 * Time: 15:13
 */

namespace Avro\Test;

use Avro\Record\AvroErrorRecord;

class AlwaysRaised extends AvroErrorRecord {
  /** @var string */
  protected $exception;

  /**
   * TestException constructor.
   * @param string $exception
   */
  public function __construct($exception = null) {
    parent::__construct(null);
    $this->exception = $exception;
  }

  /**
   * @return string
   */
  public function getException() {
    return $this->exception;
  }

  /**
   * @param string $exception
   */
  public function setException($exception) {
    $this->exception = $exception;
  }

  public static function getName() {
    return 'AlwaysRaised';
  }
}