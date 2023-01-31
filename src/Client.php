<?php

declare(strict_types=1);
/**
 * ES客户端
 */

namespace Janartist\Elasticsearch;

use Elasticsearch\ClientBuilder;
use Hyperf\Guzzle\RingPHP\PoolHandler;
use Hyperf\Utils\Coroutine;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ContainerInterface;

class Client
{
    /**
     * @var ContainerInterface
     */
    protected $container;
    /**
     * @var array
     */
    protected $config;

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
        $this->config = $container->get(ConfigInterface::class)->get('elasticsearch', []);
    }

    /**
     * @param string $group
     * @return \Elasticsearch\Client
     */
    public function create(string $group = 'default') :\Elasticsearch\Client
    {
        $config = $this->config[$group] ?? [];
        if (empty($config)) {
            throw new InvalidConfigException('elasticsearch config empty!');
        }
        $builder = ClientBuilder::create();
        if (Coroutine::inCoroutine()) {
            $handler = make(PoolHandler::class, [
                'option' => [
                    'max_connections' => $config['max_connections'] ?? 50,
                    'timeout' => $config['timeout'] ?? 0,
                ],
            ]);
            $builder->setHandler($handler);
        }

        return $builder->setHosts($config['hosts'])
            ->build();
    }
}
