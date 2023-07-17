package org.example.geotool;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * 读取geoip2的city info并打印
 */
public class GeoLiteCityInfoReader {
    public static void main(String[] args) throws IOException, GeoIp2Exception {
        if (args.length < 2) {
            System.out.println("java -jar 文件名 ip");
            return;
        }
        String filePath = args[0];
        String ip = args[1];
        DatabaseReader.Builder builder = new DatabaseReader.Builder(new File(filePath));
        DatabaseReader build = builder.build();
        InetAddress inetAddress = InetAddress.getByName(ip);
        CityResponse city = build.city(inetAddress);
        System.out.println(city);

    }
}
