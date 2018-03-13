package com.criteo.hadoop.garmadon.protocol;

import org.junit.Test;

public class ProtocolVersionTest {
    @Test
    public void Protocol_should_validate_version_of_server_via_greetings_frame() throws ProtocolVersion.InvalidFrameException, ProtocolVersion.InvalidProtocolVersionException {
        //Given
        byte[] greeting = new byte[]{0, 0, 'V', ProtocolVersion.VERSION};

        // when
        ProtocolVersion.checkVersion(greeting);
    }

    @Test(expected = ProtocolVersion.InvalidProtocolVersionException.class)
    public void Protocol_should_invalidate_bad_version_of_server_via_greetings() throws ProtocolVersion.InvalidFrameException, ProtocolVersion.InvalidProtocolVersionException {
        byte[] greeting = new byte[]{0, 0, 'V', ProtocolVersion.VERSION + 1};
        ProtocolVersion.checkVersion(greeting);
    }

    @Test(expected = ProtocolVersion.InvalidFrameException.class)
    public void Protocol_should_refuse_bad_greetings_frame_1() throws ProtocolVersion.InvalidFrameException, ProtocolVersion.InvalidProtocolVersionException {
        //Given
        byte[] bad = new byte[]{1, 0, 'V', 1};

        //when
        ProtocolVersion.checkVersion(bad);
    }

    @Test(expected = ProtocolVersion.InvalidFrameException.class)
    public void Protocol_should_refuse_bad_greetings_frame_2() throws ProtocolVersion.InvalidFrameException, ProtocolVersion.InvalidProtocolVersionException {
        //Given
        byte[] bad = new byte[]{0, 1, 'V', 1};

        //when
        ProtocolVersion.checkVersion(bad);
    }

    @Test(expected = ProtocolVersion.InvalidFrameException.class)
    public void Protocol_should_refuse_bad_greetings_frame_3() throws ProtocolVersion.InvalidFrameException, ProtocolVersion.InvalidProtocolVersionException {
        //Given
        byte[] bad = new byte[]{0, 0, 3, 1};

        //when
        ProtocolVersion.checkVersion(bad);
    }


}
