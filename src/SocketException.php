<?php


namespace rabbit\db\redis;

use rabbit\db\Exception;

/**
 * Class SocketException
 * @package rabbit\db\redis
 */
class SocketException extends Exception
{
    /**
     * @return string the user-friendly name of this exception
     */
    public function getName()
    {
        return 'Redis Socket Exception';
    }
}
