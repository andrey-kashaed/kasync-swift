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

public final class Sluice: @unchecked Sendable {
    
    public struct Pass {
        let enterId: UInt64
    }
    
    public init(capacity: Int = 1) {
        passingCapacity = capacity
    }
    
    private var enterContinuations: [UInt64: CheckedContinuation<Void, Never>] = [:]
    private var passQueue: [Pass] = []
    private var passingCounter: Int = 0
    private let passingCapacity: Int
    private let lock = NSRecursiveLock()
    
    @discardableResult
    @inline(__always)
    public func restricted<T>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
        await enter()
        defer { exit() }
        return try await operation()
    }
    
    private func enter() async {
        let enterId = UInt64.random(in: UInt64.min...UInt64.max)
        await withCheckedContinuation() { [weak self] (continuation: CheckedContinuation<Void, Never>) -> Void in
            guard let self else { return }
            self.addEnterContinuation(continuation, enterId: enterId)
            self.queuePass(Pass(enterId: enterId))
            self.dispatchContinuations()
        }
    }
    
    private func exit() {
        decreasePassingCounter()
        dispatchContinuations()
    }
    
    private func addEnterContinuation(_ continuation: CheckedContinuation<Void, Never>, enterId: UInt64) {
        lock.withLock {
            enterContinuations[enterId] = continuation
        }
    }
    
    private func removeEnterContinuation(enterId: UInt64) -> CheckedContinuation<Void, Never>? {
        lock.withLock {
            enterContinuations.removeValue(forKey: enterId)
        }
    }
    
    private func queuePass(_ pass: Pass) {
        lock.withLock {
            passQueue.append(pass)
        }
    }
    
    private func dequeuePass() -> Pass? {
        lock.withLock {
            passQueue.popFirst()
        }
    }
    
    private func dispatchContinuations() {
        lock.withLock {
            if passingCounter >= passingCapacity { return }
            guard let pass = dequeuePass() else { return }
            guard let enterContinuation = removeEnterContinuation(enterId: pass.enterId) else { return }
            increasePassingCounter()
            enterContinuation.resume()
        }
    }
    
    private func increasePassingCounter() {
        lock.withLock {
            passingCounter += 1
        }
    }
    
    private func decreasePassingCounter() {
        lock.withLock {
            passingCounter -= 1
        }
    }
    
}
