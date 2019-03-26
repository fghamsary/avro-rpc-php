<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:57
 */

namespace Avro\IO;

use Avro\Exception\AvroException;
use Avro\Schema\AvroSchema;

/**
 * Class AvroIOSchemaMatchException Exceptions arising from incompatibility between reader and writer schemas.
 * @package Avro\IO
 */
class AvroIOSchemaMatchException extends AvroException {

  /**
   * @param AvroSchema $writersSchema
   * @param AvroSchema $readersSchema
   */
  function __construct(AvroSchema $writersSchema, AvroSchema $readersSchema) {
    parent::__construct(
      sprintf("Writer's schema %s and Reader's schema %s do not match.", $writersSchema, $readersSchema)
    );
  }

}
