<?php
declare(strict_types=1);

namespace rabbit\db\redis\Commands;


use Predis\Command\TransactionMulti;

/**
 * Class MUTIL
 * @package rabbit\db\redis
 */
class MUTIL extends TransactionMulti
{
    use ClusterSlot;
}