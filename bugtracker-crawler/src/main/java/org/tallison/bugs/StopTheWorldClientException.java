package org.tallison.bugs;

import java.io.IOException;

public class StopTheWorldClientException extends ClientException {

    public StopTheWorldClientException(String msg) {
        super(msg);
    }

    public StopTheWorldClientException(Exception e) {
        super(e);
    }

    public StopTheWorldClientException(String url, IOException e) {
        super(url, e);
    }
}
