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
    
    private let passCapacity: Int
    private var passCounter: Int = 0
    private var continuations: [CheckedContinuation<Void, Never>] = []
    private let lock = NSRecursiveLock()
    
    public init(passCapacity: Int) {
        self.passCapacity = passCapacity
    }
    
    public var awaitingParties: Int {
        lock.withLock { continuations.count }
    }
    
    public var passingParties: Int {
        lock.withLock { passCounter }
    }
    
    public func withTransaction(_ transaction: (Sluice) -> Void) {
        lock.withLock { transaction(self) }
    }
    
    @discardableResult
    @inline(__always)
    public func restricted<T>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
        await enter()
        defer { exit() }
        return try await operation()
    }
    
    private func enter() async {
        await withCheckedContinuation() { [weak self] (continuation: CheckedContinuation<Void, Never>) -> Void in
            self?.withTransaction { sluice in
                sluice.enqueueContinuationUnsafe(continuation)
                sluice.resolveContinuationsUnsafe()
            }
        }
    }
    
    private func exit() {
        lock.withLock {
            passCounter -= 1
            resolveContinuationsUnsafe()
        }
    }
    
    private func enqueueContinuationUnsafe(_ continuation: CheckedContinuation<Void, Never>) {
        continuations.append(continuation)
    }
    
    private func resolveContinuationsUnsafe() {
        while continuations.count > 0 && passCounter < passCapacity {
            let continuation = continuations.removeFirst()
            continuation.resume()
            passCounter += 1
        }
    }
    
}
