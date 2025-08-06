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

    /**
     * This test demonstrates how to use the `getProperty` utility method to fetch a Long value
     * from a resource. We simulate a case where the resource contains the string "456"
     * for the key "triggerRangeMax". The test checks if the value is successfully retrieved,
     * properly converted to a Long, and matches the expected result (456L).
     * <p>
     * This is useful for understanding how type-safe property retrieval works with Optional values.
     */

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

    /**
     * This test demonstrates how the `getProperty` utility method retrieves and parses a Boolean
     * value from a resource. It simulates a case where the resource returns the string "true"
     * for the key "heartbeat". Since `ScannerUtils` internally converts the key to an IReference,
     * we mock the resource response based on that reference.
     * <p>
     * The test verifies that:
     * 1. The value is successfully retrieved (i.e., the Optional is present),
     * 2. The returned value is parsed correctly as a Boolean,
     * 3. The parsed value matches the expected result: true.
     * <p>
     * This helps clarify how type-safe property extraction and automatic key conversion work
     * in the utility method.
     */

    @Test
    void testGetProperty_ReturnsBooleanValue() {
        // Since `ScannerUtils.getProperty` internally converts the key string ("heartbeat") to an IReference,
        // we must mock the response using that exact IReference, not just the raw key string.
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

    /**
     * This test shows how the `getProperty` method behaves when the requested property
     * is not available or explicitly set to null in the resource.
     * <p>
     * We simulate this by returning null for the key "triggerRangeMax". The test ensures that:
     * 1. The method handles the null value gracefully,
     * 2. It returns an empty Optional instead of throwing an exception.
     * <p>
     * This is important for ensuring robustness when dealing with optional configuration values.
     */
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
