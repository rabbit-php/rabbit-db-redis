<?php
declare(strict_types=1);

namespace rabbit\db\redis\Commands;

use Predis\Command\TransactionExec;

/**
 * Class EXEC
 * @package rabbit\db\redis\ExtCommand
 */
class EXEC extends TransactionExec
{
    use ClusterSlot;
}