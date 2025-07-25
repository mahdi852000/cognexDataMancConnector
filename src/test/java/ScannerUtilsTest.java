import lombok.extern.slf4j.Slf4j;
import net.enilink.komma.core.IReference;
import org.example.akka.extra.IResource;

import org.example.akka.extra.LOGISTICS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import org.example.akka.utils.ScannerUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
public class ScannerUtilsTest {


    private IResource mockResource;
    private IReference mockRef;

    @BeforeEach
    public void setUp() {
        mockResource = mock(IResource.class);
        mockRef = mock(IReference.class);
    }

    @Test
    void testGetProperty_ReturnsLongValue() {
        IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("triggerRangeMax");
        when(mockResource.getSingle(expectedRef)).thenReturn("456");

        Optional<Long> result = ScannerUtils.getProperty(mockResource, Long.class, "triggerRangeMax");

        if (result.isPresent()) {
            System.out.println("Returned value: " + result.get());
        } else {
            System.out.println("No value returned!");
        }

        assertTrue(result.isPresent());
        assertEquals(456L, result.get());

        log.info("getProperty returned: {}", result);

        System.out.println("Everything is fine");

    }


    @Test
    void testGetProperty_ReturnsBooleanValue() {
        // چون ScannerUtils خودش string رو به IReference تبدیل می‌کنه، باید روی IReference ساخته‌شده از "heartbeat" mock کنیم
        IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("heartbeat");
        when(mockResource.getSingle(expectedRef)).thenReturn("true");

        Optional<Boolean> result = ScannerUtils.getProperty(mockResource, Boolean.class, "heartbeat");

        if (result.isPresent()) {
            System.out.println("Parsed boolean value: " + result.get());
        } else {
            System.out.println("No value returned for boolean property.");
        }
        assertTrue(result.isPresent());
        assertTrue(result.get());
    }


    @Test
    void testGetProperty_ReturnsEmptyIfNull() {

        IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("triggerRangeMax");
        when(mockResource.getSingle(expectedRef)).thenReturn(null);

        Optional<Long> result = ScannerUtils.getProperty(mockResource, Long.class, "triggerRangeMax");

        System.out.println("Result of getProperty when value is null: " + result);

        assertFalse(result.isPresent());
    }

    @Test
    void testGetProperty_InvalidConversion() {
        IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("notANumber");
        when(mockResource.getSingle(expectedRef)).thenReturn("notANumber");

        Optional<Long> result = ScannerUtils.getProperty(mockResource, Long.class, "notANumber");

        System.out.println("Result of getProperty when value is not valid: " + result);

        assertFalse(result.isPresent());
    }
}
