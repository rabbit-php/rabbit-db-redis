<?php
declare(strict_types=1);

namespace rabbit\db\redis;

/**
 * Trait ClusterTrait
 * @package rabbit\db\redis
 */
trait ClusterTrait
{
    /** @var bool */
    protected $cluster = false;

    /**
     * @return bool
     */
    public function getCluster(): bool
    {
        return $this->cluster;
    }
}