<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:56
 */

namespace Avro\IO;

use Avro\Exception\AvroException;
use Avro\Schema\AvroSchema;

/**
 * Exceptions arising from writing or reading Avro data.
 * @package Avro\IO
 */
class AvroIOTypeException extends AvroException {

  /**
   * @param AvroSchema $expectedSchema
   * @param mixed $datum
   */
  public function __construct(AvroSchema $expectedSchema, $datum) {
    parent::__construct(
      sprintf('The datum %s is not an example of schema %s', var_export($datum, true), $expectedSchema)
    );
  }

}