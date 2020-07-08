<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis\Commands;


use Predis\Command\TransactionMulti;

/**
 * Class MUTIL
 * @package Rabbit\DB\Redis\Commands
 */
class MUTIL extends TransactionMulti
{
    use ClusterSlot;
}