package com.threecube.phoenix.writer.result;

/**
 * Created by Mike Ding on 2015/11/16.
 */
public class AbsractResult {

    private boolean isSuccess;

    private int errCode;

    private String errMsg;

    public boolean isSuccess() {
        return isSuccess;
    }

    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setIsSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }
}
