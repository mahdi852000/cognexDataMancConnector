import net.enilink.komma.core.IEntityManager;
import net.enilink.komma.core.IReference;
import net.enilink.komma.core.URI;
import org.example.akka.extra.IResource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mock implementation of IResource used to simulate RDF-based resource querying.
 * Returns a mock IReference and mock URI for ScannerActor's use.
 */
 class DummyResource implements IResource {
    private final IReference ref;
    private final URI uri;

    public DummyResource() {
        this(mock(URI.class));
    }

    public DummyResource(URI uri) {
        this.uri = uri;
        ref = mock(IReference.class);
        when(ref.getURI()).thenReturn(uri);
    }

    public Object getSingle(IReference var1) { return ref; }
    public <T> T as(Class<T> aClass) { return null; }
    public IEntityManager getEntityManager() { return null; }
    public void refresh() {}
    public URI getURI() { return uri; }
    public IReference getReference() { return ref; }
}