package org.example.akka.message;

import org.example.akka.extra.IResource;

public interface ScannerEventListener {

    void onCodeScanned(IResource resource, String code);
    void onOccupationChange(IResource resource, Boolean occupation);
}
