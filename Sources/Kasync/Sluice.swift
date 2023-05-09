//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

import Foundation

public class Sluice {
    
    public struct Pass {
        let enterId: UInt64
    }
    
    public init(capacity: Int = 1) {
        passingCapacity = capacity
    }
    
    private var enterContinuations: [UInt64: CheckedContinuation<Void, Error>] = [:]
    private var passQueue: [Pass] = []
    private var passingCounter: Int = 0
    private let passingCapacity: Int
    private let lock = NSRecursiveLock()
    
    public func enter() async throws {
        let enterId = UInt64.random(in: UInt64.min...UInt64.max)
        defer {
            removeEnterContinuation(enterId: enterId)
        }
        try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Void, Error>, cancellation: Cancellation) -> Void in
            cancellation.onCancel = { [weak self] in
                self?.removeEnterContinuation(enterId: enterId)?.resume(throwing: CancellationError())
            }
            guard let self else { return }
            self.addEnterContinuation(continuation, enterId: enterId)
            self.queuePass(Pass(enterId: enterId))
            self.dispatchPass()
        }
        try Task.checkCancellation()
    }
    
    public func exit() {
        decreasePassingCounter()
        dispatchPass()
    }
    
    private func addEnterContinuation(_ continuation: CheckedContinuation<Void, Error>, enterId: UInt64) {
        lock.synchronized {
            enterContinuations[enterId] = continuation
        }
    }
    
    @discardableResult
    private func removeEnterContinuation(enterId: UInt64) -> CheckedContinuation<Void, Error>? {
        lock.synchronized {
            enterContinuations.removeValue(forKey: enterId)
        }
    }
    
    private func queuePass(_ pass: Pass) {
        lock.synchronized {
            passQueue.append(pass)
        }
    }
    
    private func dequeuePass() -> Pass? {
        lock.synchronized {
            passQueue.popFirst()
        }
    }
    
    private func dispatchPass() {
        lock.synchronized {
            if passingCounter >= passingCapacity { return }
            guard let pass = dequeuePass() else { return }
            guard let enterContinuation = removeEnterContinuation(enterId: pass.enterId) else { return }
            increasePassingCounter()
            enterContinuation.resume()
        }
    }
    
    private func increasePassingCounter() {
        lock.synchronized {
            passingCounter += 1
        }
    }
    
    private func decreasePassingCounter() {
        lock.synchronized {
            passingCounter -= 1
        }
    }
    
}

public extension Sluice {
    @discardableResult
    @inline(__always)
    func synchronized<T>(_ closure: () async throws -> T) async throws -> T {
        try await enter()
        defer { exit() }
        return try await closure()
    }
}
