package pl.bdabek;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.util.Util;

public class SystemInformation {

    private final String serialNumber;
    private final HardwareAbstractionLayer hardwareAbstractionLayer;

    public SystemInformation() {
        SystemInfo si = new SystemInfo();
        hardwareAbstractionLayer = si.getHardware();
        serialNumber = hardwareAbstractionLayer.getComputerSystem().getSerialNumber();
    }

    double getCpuUsage() {
        CentralProcessor processor = hardwareAbstractionLayer.getProcessor();
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        Util.sleep(1000);
        double cpuUsage = Math.floor((processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100) * 100) / 100;
        System.out.println("CPU load: " + cpuUsage + "%");
        return cpuUsage;
    }

    double getMemoryUsage() {
        GlobalMemory memory = hardwareAbstractionLayer.getMemory();
        long max = memory.getTotal();
        long available = memory.getAvailable();
        double ramUsage = 100 - (available * 100f / max);
        double ramUsageWithPrecision = Math.floor(ramUsage * 100) / 100;
        System.out.println("RAM usage: " + ramUsageWithPrecision + "%");
        return ramUsageWithPrecision;
    }

    public String getSerialNumber() {
        return serialNumber;
    }
}
