<?php

namespace FpDbTest\src;

use Exception;

class DatabaseTest
{
    private DatabaseInterface $db;


    public function __construct(DatabaseInterface $db)
    {
        $this->db = $db;
    }

    public function testBuildQuery(): void
    {
        $results = [];

        $results[] = $this->db->buildQuery('SELECT name FROM users WHERE user_id = 1');

        $results[] = $this->db->buildQuery(
            'SELECT * FROM users WHERE name = ? AND block = 0',
            ['Jack']
        );

        $results[] = $this->db->buildQuery(
            'SELECT ?# FROM users WHERE user_id = ?d AND block = ?d',
            [['name', 'email'], 2, true]
        );

        $results[] = $this->db->buildQuery(
            'UPDATE users SET ?a WHERE user_id = -1',
            [['name' => 'Jack', 'email' => null]]
        );

        foreach ([null, true] as $block) {
            $results[] = $this->db->buildQuery(
                'SELECT name FROM users WHERE ?# IN (?a){ AND block = ?d}',
                ['user_id', [1, 2, 3], $block ?? $this->db->skip()]
            );
        }

        // Добавил пару тестов, чтоб "наверняка"
        $results[] = $this->db->buildQuery(
            'SELECT * FROM users WHERE test = ? AND name = ? AND x1 = ? AND block = 0',
            ['Jack', 34, null]
        );
        // В этом примере "удаляется" только один условный блок
        $results[] = $this->db->buildQuery(
            'SELECT name FROM users WHERE ?# IN (?a){ AND block = ?d}{ AND test = ?}',
            ['user_id', [1, 2, 3], true, $this->db->skip()]
        );

        // В изначальной версии строки в массиве были в одинарных кавычках,
        // из-за этого, обратный слеш игнорировался и сравнение происходило
        // некорректно в экранированных строках
        $correct = [
            "SELECT name FROM users WHERE user_id = 1",
            "SELECT * FROM users WHERE name = \'Jack\' AND block = 0",
            "SELECT `name`, `email` FROM users WHERE user_id = 2 AND block = 1",
            "UPDATE users SET `name` = \'Jack\', `email` = NULL WHERE user_id = -1",
            "SELECT name FROM users WHERE `user_id` IN (1, 2, 3)",
            "SELECT name FROM users WHERE `user_id` IN (1, 2, 3) AND block = 1",
            "SELECT * FROM users WHERE test = \'Jack\' AND name = 34 AND x1 = NULL AND block = 0",
            "SELECT name FROM users WHERE `user_id` IN (1, 2, 3) AND block = 1",
        ];
        // Для проверок, в случае ошибки в тесте
//        $k = 1;
//        foreach (str_split($correct[$k]) as $i=>$char) {
//                if($correct[$k][$i] != $results[$k][$i]){
//                    var_dump($correct[$k][$i], $results[$k][$i], $results[$k], $correct[$k]);
//                    return;
//                }
//        }
        if ($results !== $correct) {
            throw new Exception('Failure.');
        }
    }
}
