// Code generated by MockGen. DO NOT EDIT.
// Source: net (interfaces: Listener)

// Package transport is a generated GoMock package.
package transport_test

import (
	gomock "github.com/golang/mock/gomock"
	net "net"
	reflect "reflect"
)

// mockNetListener is a mock of Listener interface
type mockNetListener struct {
	ctrl     *gomock.Controller
	recorder *mockNetListenerMockRecorder
}

// mockNetListenerMockRecorder is the mock recorder for mockNetListener
type mockNetListenerMockRecorder struct {
	mock *mockNetListener
}

// newMockNetListener creates a new mock instance
func newMockNetListener(ctrl *gomock.Controller) *mockNetListener {
	mock := &mockNetListener{ctrl: ctrl}
	mock.recorder = &mockNetListenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *mockNetListener) EXPECT() *mockNetListenerMockRecorder {
	return m.recorder
}

// Accept mocks base method
func (m *mockNetListener) Accept() (net.Conn, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Accept")
	ret0, _ := ret[0].(net.Conn)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Accept indicates an expected call of Accept
func (mr *mockNetListenerMockRecorder) Accept() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Accept", reflect.TypeOf((*mockNetListener)(nil).Accept))
}

// Addr mocks base method
func (m *mockNetListener) Addr() net.Addr {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Addr")
	ret0, _ := ret[0].(net.Addr)
	return ret0
}

// Addr indicates an expected call of Addr
func (mr *mockNetListenerMockRecorder) Addr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Addr", reflect.TypeOf((*mockNetListener)(nil).Addr))
}

// Close mocks base method
func (m *mockNetListener) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *mockNetListenerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*mockNetListener)(nil).Close))
}
