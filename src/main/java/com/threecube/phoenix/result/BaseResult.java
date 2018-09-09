package com.threecube.phoenix.result;

import java.io.Serializable;

/**
 * Created by Mike Ding on 2015/11/24.
 */
public class BaseResult implements Serializable {

    private boolean success;

    private int errCode;

    private String errMsg;

    public boolean isSuccess() {
        return success;
    }

    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }
}
