package examples.redis;

import java.util.Map;

import redis.clients.jedis.Jedis;

public class RedisDemo {
    public static void main(String[] args) {
        System.out.println("connecting...");
        Jedis jedis = new Jedis("localhost", 6379);
        System.out.println("Successfully connected");
        // insert Student.Scofield
        jedis.hset("Student.Scofield", "English", "45");
        jedis.hset("Student.Scofield", "Math", "89");
        jedis.hset("Student.Scofield", "Computer", "100");
        Map<String, String> values = jedis.hgetAll("Student.Scofield");
        for (Map.Entry<String, String> entry : values.entrySet())
            System.out.println(entry.getKey() + ": " + entry.getValue());
        // get 
        String value = jedis.hget("Student.Scofield", "English");
        System.out.println("Scofield's English score: " + value);
        jedis.close();
    }
}
