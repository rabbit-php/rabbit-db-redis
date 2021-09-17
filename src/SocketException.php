<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Rabbit\Base\Core\Exception;

/**
 * Class SocketException
 * @package Rabbit\DB\Redis
 */
class SocketException extends Exception
{
    public function getName(): string
    {
        return 'Redis Socket Exception';
    }
}
