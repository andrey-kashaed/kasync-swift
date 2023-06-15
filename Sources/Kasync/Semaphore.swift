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

public enum SemaphoreError: Error {
    case disabledSemaphore
    case resetSemaphore
}

extension SemaphoreError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .disabledSemaphore:
            return "Semaphore is disabled"
        case .resetSemaphore:
            return "Semaphore is reset"
        }
    }
}

public final class Semaphore: @unchecked Sendable {
    
    public let initialPermits: Int
    private var enabled: Bool
    private var permits: Int
    private var continuations: [CheckedContinuation<Void, Error>] = []
    private let lock = NSRecursiveLock()
    
    public init(initialPermits: Int, enabled: Bool = true) {
        self.initialPermits = initialPermits
        self.enabled = enabled
        self.permits = initialPermits
    }
    
    public var availablePermits: Int {
        lock.withLock { permits }
    }
    
    public var awaitingParties: Int {
        lock.withLock { continuations.count }
    }
    
    public var isEnabled: Bool {
        lock.withLock { enabled }
    }
    
    public func withTransaction(_ transaction: (Semaphore) -> Void) {
        lock.withLock { transaction(self) }
    }
    
    public func await(enabledAfterCancellation: Bool = false) async throws {
        try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Void, Error>, cancellation: Cancellation) -> Void in
            self?.withTransaction { semaphore in
                if !semaphore.enabled { continuation.resume(throwing: SemaphoreError.disabledSemaphore) }
                semaphore.enqueueContinuationUnsafe(continuation)
                semaphore.resolveContinuationsUnsafe()
                cancellation.onCancel = { [weak semaphore] in
                    semaphore?.reset(enabled: enabledAfterCancellation)
                }
            }
        }
    }
    
    public func signal() throws {
        try lock.withLock {
            if !enabled { throw SemaphoreError.disabledSemaphore }
            permits += 1
            resolveContinuationsUnsafe()
        }
    }
    
    public func signal(permits: Int) throws {
        try lock.withLock {
            if !enabled { throw SemaphoreError.disabledSemaphore }
            self.permits += permits
            resolveContinuationsUnsafe()
        }
    }
    
    public func reset(enabled: Bool, error: Error = SemaphoreError.resetSemaphore) {
        lock.withLock {
            self.enabled = enabled
            permits = initialPermits
            for continuation in continuations {
                continuation.resume(throwing: error)
            }
            continuations.removeAll()
        }
    }
    
    private func enqueueContinuationUnsafe(_ continuation: CheckedContinuation<Void, Error>) {
        continuations.append(continuation)
    }
    
    private func resolveContinuationsUnsafe() {
        while continuations.count > 0 && permits > 0 {
            let continuation = continuations.removeFirst()
            continuation.resume()
            permits -= 1
        }
    }
    
}
