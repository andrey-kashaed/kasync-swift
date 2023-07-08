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

public final actor Mutex {
    
    private lazy var sluice: Sluice = { Sluice(passCapacity: 1) }()
    private var sluicePartyCounter = 0
    
    public init() {}

    @discardableResult
    public func atomic<R: Sendable>(_ operation: @Sendable () throws -> R) async rethrows -> R {
        guard sluicePartyCounter > 0 else {
            return try operation()
        }
        sluicePartyCounter += 1
        defer { sluicePartyCounter -= 1 }
        return try await sluice.restricted {
            try operation()
        }
    }
    
    @discardableResult
    public func atomic<R: Sendable>(_ operation: @Sendable () async throws -> R) async rethrows -> R {
        sluicePartyCounter += 1
        defer { sluicePartyCounter -= 1 }
        return try await sluice.restricted {
            try await operation()
        }
    }

}
